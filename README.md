# scry_engine_mongodb_driver

A real [`Scry.Core.EngineBehaviour`](https://github.com/joetjen/scry_core)
implementation over [MongoDB](https://www.mongodb.com/), via
[`mongodb_driver`](https://hex.pm/packages/mongodb_driver). Replaces
`scry_document`'s own in-memory reference implementation
(`Scry.Document.Executor` -- a plain `%{path => rows}` map held
entirely in memory) with genuine collection-backed `DEEP`/`PARENT`/
`SIBLINGS`/`ANCESTORS` execution against a real document store -- the
`document` kind's first real adapter, the same gap
[`scry_engine_neo4j`](https://github.com/joetjen/scry_engine_neo4j)
closed for `graph` one round earlier.

Like `scry_search`/`scry_graph` (replaced by
`scry_engine_elasticsearch`/`scry_engine_redisearch`/
`scry_engine_neo4j`), `document` has no separate engine tier in its own
reference form -- `Scry.Document.Executor` takes a bespoke
`Scry.Document.Conn.t()` directly, because `DEEP`/`PARENT`/`SIBLINGS`/
`ANCESTORS` all need access to the *whole* keyed document space, not
just the rows behind one already-resolved source. This package
implements `Scry.Core.EngineBehaviour` directly instead.

Source: <https://github.com/joetjen/scry_engine_mongodb_driver>. Specs
live in the separate [`scry`](https://github.com/joetjen/scry)
repository; the behaviour this implements lives in
[`scry_core`](https://github.com/joetjen/scry_core).

## Usage

```elixir
{:ok, conn} = Scry.Engine.MongoDB.Conn.open(url: "mongodb://localhost:27017/mydb")

{:ok, query} = Scry.Core.parse(~s(SELECT library.fiction.book { title, PARENT { name } }))
{:ok, cursor} = Scry.Core.Executor.run(query, Scry.Engine.MongoDB, conn)
rows = Scry.Core.Cursor.to_list(cursor)
# rows == [%{"title" => "Dune", "parent" => %{"name" => "Fiction"}}, ...]
```

Creating collections/documents is entirely the caller's own job -- this
package is schema-agnostic and issues nothing but ordinary `find`/
`findOne`/`listCollections` reads.

### Local development / running the test suite

```sh
docker run -d --name scry-mongo -p 27017:27017 mongo:7
```

## Collection-per-tree-key, not one big collection

`Scry.Document.Conn`'s own reference model keys rows by tree position
-- `["library", "catalog", "fiction"]` is the parent of `["library",
"catalog", "fiction", "special_editions"]`, the same convention a
filesystem path uses. The most direct real-backend translation of that
model is **one real MongoDB collection per tree key**, its own name the
key's segments joined with `.` -- confirmed directly: a real
`.`-containing collection name (`library.catalog.fiction`) inserts and
queries correctly against a real MongoDB 7 server, no escaping or
workaround needed. `DEEP`'s own cross-key matching and `PARENT`/
`SIBLINGS`/`ANCESTORS`'s own relative-key resolution both become
"enumerate every real collection name (`db.listCollections()`), filter
client-side by the identical logic the reference `Executor` already
uses, then fetch the matched collection(s)" -- the same shape
`scry_engine_neo4j`'s own `SHORTEST`-backed `VIA` translation has, just
with `listCollections`/`find` standing in for Cypher's own `MATCH`.

A stated, reference-scale simplification: a tree-key segment containing
a literal `.` of its own would collide with this join convention -- the
identical caveat `scry_engine_neo4j` already states for a multi-segment
`VIA` edge name joined the same way.

## No pushdown at all -- everything but hierarchy resolution is generic

Every one of `WHERE`/`GROUP BY`/aggregates/`HAVING`/`DISTINCT`/
`ORDER BY`/`LIMIT`/`OFFSET`/projection -- at the top level, and inside a
`PARENT`/`SIBLINGS`/`ANCESTORS` body -- applies *generically*, via
`Scry.Core.QueryOps.run_flat/3`, never translated into a MongoDB query
filter at all. This is the same "reference engine's own predicate
evaluator, not a hand-rolled translator" posture `scry_engine_neo4j`
already established for `VIA`'s own `WHERE`/`ORDER BY`/etc., applied
here even more broadly: MongoDB's own filter query language has nothing
structural to contribute here at all -- the only real adapter-specific
work is knowing *which collection(s)* to read. This also sidesteps the
schemaless-pushdown-safety question `scry_engine_elasticsearch`/
`scry_engine_redisearch`/`scry_engine_neo4j` each already answered by
declining pushdown -- there's no pushdown here to question the safety
of in the first place. The real, stated cost: every document in a
matched collection is fetched before `WHERE`/`LIMIT`/etc. narrow it,
not optimized for a large collection.

`_id` (a real `%BSON.ObjectId{}` struct `mongodb_driver` always
attaches) is stripped from every row before it reaches any of this
pipeline -- the reference model's own rows have no implicit `"_id"`
field at all.

`PARENT`/`SIBLINGS`/`ANCESTORS`/`DEEP` combined with `GROUP BY`, and
`%Scry.Core.CombinedQuery{}`, decline exactly like the reference
(`{:unsupported, :pseudo_field_with_group_by}`/`{:unsupported,
:combined_query}`, atom names kept byte-for-byte identical to the
reference's own). A `WITH`-bound top-level `source` delegates to
`Scry.Core.QueryOps.run_document/4` rather than declining outright the
way the reference does -- `scry_document`'s own executor never had the
option (it isn't registered as an `EngineBehaviour` implementation at
all), but this package is, the same upgrade `scry_engine_neo4j` already
made over its own reference.

A collection matching no document at all -- or that doesn't exist yet
-- is simply an empty result, not an error: confirmed directly,
`Mongo.find/4` against a nonexistent collection returns `[]`. This is a
real, deliberate divergence from `scry_document`'s own reference
`resolve_source/3` (which errors for a source absent from its own
synthetic map) -- the identical divergence `scry_engine_neo4j` already
states for a label matching no node.

## One real driver finding, not assumed

`Mongo.command/3` requires its own `cmd` argument as a **keyword
list**, not a plain map -- confirmed directly: passing an ordinary
`%{dropDatabase: 1}` (or a string-keyed equivalent) crashes with a
`FunctionClauseError` inside `mongodb_driver`'s own session handling
(`Keyword.get/3` called against a plain map). `[dropDatabase: 1]` works
correctly. This package's own test suite uses this only for fixture
teardown between test runs, never inside `execute/3` itself.

## Parity testing against the reference

AGENTS.md's "Parity between multiple implementations" rule applies
directly here: `scry_document`'s reference `Executor` and this package
are two implementations of the identical `DEEP`/`PARENT`/`SIBLINGS`/
`ANCESTORS` semantics. `test/scry/engine/mongo_db/parity_test.exs`
parses one query text once (`Scry.Document.parse/1`) and runs the exact
same `%Scry.Core.Query{}` against a byte-for-byte identical fixture in
both a real MongoDB container and the reference's own in-memory `Conn`,
asserting the results agree (`scry_document` is a `only: :test`
dependency for exactly this purpose).

## Installation

```elixir
def deps do
  [
    {:scry_engine_mongodb_driver, "~> 0.1.0"}
  ]
end
```

## Documentation

Documentation is generated with [ExDoc](https://github.com/elixir-lang/ex_doc):

- Released versions are published to [HexDocs](https://hexdocs.pm) once the
  package ships, at <https://hexdocs.pm/scry_engine_mongodb_driver>.
