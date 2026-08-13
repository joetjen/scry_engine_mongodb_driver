defmodule Scry.Engine.MongoDB do
  @moduledoc """
  A real `Scry.Core.EngineBehaviour` implementation over MongoDB, via
  [`mongodb_driver`](https://hex.pm/packages/mongodb_driver) --
  actively maintained (unlike `scry_engine_neo4j`'s own `boltx`, no
  driver correction was needed here). Replaces `scry_document`'s own
  reference implementation (`Scry.Document.Executor` -- a plain
  `%{[String.t(), ...] => [row]}` map held entirely in memory) with
  genuine collection-backed `DEEP`/`PARENT`/`SIBLINGS`/`ANCESTORS`
  execution against a real document store -- the `document` kind's
  first real adapter, the same gap `scry_engine_neo4j` closed for
  `graph` one round earlier.

  Like `scry_search`/`scry_graph` (replaced by `scry_engine_
  elasticsearch`/`scry_engine_redisearch`/`scry_engine_neo4j`),
  `document` has no separate engine tier in its own reference form --
  `Scry.Document.Executor` takes a bespoke `Scry.Document.Conn.t()`
  directly, because `DEEP`/`PARENT`/`SIBLINGS`/`ANCESTORS` all need
  access to the *whole* keyed document space, not just the rows behind
  one already-resolved source. This package implements `Scry.Core.
  EngineBehaviour` directly instead.

  ## Collection-per-tree-key, not one big collection

  `Scry.Document.Conn`'s own reference model keys rows by tree
  position -- `["library", "catalog", "fiction"]` is the parent of
  `["library", "catalog", "fiction", "special_editions"]`, the same
  convention a filesystem path uses (that module's own moduledoc has
  the full reasoning; no new storage primitive, a data convention on
  top of the same `%{path => rows}` shape every reference `Conn` in
  this family already has). The most direct real-backend translation of
  that model is **one real MongoDB collection per tree key**, its own
  name the key's segments joined with `.` -- confirmed directly, not
  assumed: a real `.`-containing collection name (`library.catalog.
  fiction`) inserts and queries correctly against a real MongoDB 7
  server, no escaping or workaround needed. `DEEP`'s own cross-key
  matching and `PARENT`/`SIBLINGS`/`ANCESTORS`'s own relative-key
  resolution both become "enumerate every real collection name
  (`db.listCollections()`, via `Scry.Engine.MongoDB.Conn.collection_names/1`),
  filter client-side by the identical logic the reference `Executor`
  already uses, then fetch the matched collection(s)" -- the same shape
  `scry_engine_neo4j`'s own `SHORTEST`-backed `VIA` translation has,
  just with `listCollections`/`find` standing in for Cypher's own
  `MATCH`.

  A stated, reference-scale simplification, not silently relied upon: a
  tree-key segment containing a literal `.` of its own would collide
  with this join convention -- the identical caveat `scry_engine_neo4j`
  already states for a multi-segment `VIA` edge name joined the same
  way.

  ## No pushdown at all -- everything but hierarchy resolution is generic

  Every one of `WHERE`/`GROUP BY`/aggregates/`HAVING`/`DISTINCT`/
  `ORDER BY`/`LIMIT`/`OFFSET`/projection -- at the top level, and inside
  a `PARENT`/`SIBLINGS`/`ANCESTORS` body -- applies *generically*, via
  `Scry.Core.QueryOps.run_flat/3`, never translated into a MongoDB query
  filter at all. This is the same "reference engine's own predicate
  evaluator, not a hand-rolled translator" posture `scry_engine_neo4j`
  already established for `VIA`'s own `WHERE`/`ORDER BY`/etc., applied
  here even more broadly: unlike Neo4j (whose Cypher `MATCH` computes
  the traversal itself), MongoDB's own filter query language has
  nothing structural to contribute here at all -- the only real
  adapter-specific work is knowing *which collection(s)* to read,
  exactly mirroring `Scry.Document.Executor`'s own architecture (that
  module's own moduledoc: "Ordinary `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`/
  plain-field projection are not reimplemented here"). This also
  sidesteps the schemaless-pushdown-safety question `scry_engine_
  elasticsearch`/`scry_engine_redisearch`/`scry_engine_neo4j` each
  already answered by declining pushdown -- there's no pushdown here to
  question the safety of in the first place. The real, stated cost is
  the same one every adapter in this family that takes this posture
  already states: every document in a matched collection is fetched
  before `WHERE`/`LIMIT`/etc. narrow it, not optimized for a large
  collection.

  `_id` (a real `%BSON.ObjectId{}` struct `mongodb_driver` always
  attaches) is stripped from every row before it reaches any of this
  pipeline -- `Scry.Engine.MongoDB.Conn`'s own moduledoc has the full
  reasoning (the reference model's own rows have no implicit `"_id"`
  field at all).

  `PARENT`/`SIBLINGS`/`ANCESTORS`/`DEEP` combined with `GROUP BY`, and
  `%Scry.Core.CombinedQuery{}`, decline exactly like the reference
  (`{:unsupported, :pseudo_field_with_group_by}`/`{:unsupported,
  :combined_query}` -- the atom names, `pseudo_field_with_group_by`
  included, are kept byte-for-byte identical to the reference's own,
  not renamed, for direct error-shape parity). A `WITH`-bound top-level
  `source` delegates to `Scry.Core.QueryOps.run_document/4` rather than
  declining outright the way the reference does -- `scry_document`'s
  own `Scry.Document.Executor` never had the option (it isn't
  registered as an `EngineBehaviour` implementation at all), but this
  package is, the same upgrade `scry_engine_neo4j` already made over
  its own reference.

  A collection matching no document at all -- or that doesn't exist yet
  -- is simply an empty result, not an error: confirmed directly,
  `Mongo.find/4` against a nonexistent collection returns `[]`, no error
  of any kind (MongoDB collections are implicitly created on first
  write, so "doesn't exist" and "exists but empty" are indistinguishable
  from a reader's own perspective). This is a real, deliberate
  divergence from `scry_document`'s own reference `resolve_source/3`
  (which errors for a source absent from its own synthetic map) -- the
  identical divergence `scry_engine_neo4j` already states for a label
  matching no node.

  ## One real driver finding, not assumed

  `Mongo.command/3` requires its own `cmd` argument as a **keyword
  list**, not a plain map -- confirmed directly: passing an ordinary
  `%{dropDatabase: 1}` (or a string-keyed equivalent) crashes with a
  `FunctionClauseError` inside `mongodb_driver`'s own session handling
  (`Keyword.get/3` called against a plain map). `[dropDatabase: 1]`
  works correctly. This package's own test suite uses this only for
  fixture teardown between test runs, never inside `execute/3` itself
  (no ordinary query construct this package supports needs a raw
  MongoDB command at all).
  """

  @behaviour Scry.Core.EngineBehaviour

  alias Scry.Core.{CombinedQuery, Query, QueryOps}
  alias Scry.Engine.MongoDB.Conn

  @document_key_field "__scry_mongodb_key__"

  @impl true
  def execute(conn, %CombinedQuery{} = combined, params),
    do: QueryOps.run_document(conn, combined, params, __MODULE__)

  def execute(%Conn{} = conn, %Query{} = query, params) do
    if with_bound_source?(query) do
      QueryOps.run_document(conn, query, params, __MODULE__)
    else
      run(conn, query, params)
    end
  end

  defp with_bound_source?(%Query{source: [name], with_bindings: with_bindings}),
    do: Map.has_key?(with_bindings, name)

  defp with_bound_source?(_query), do: false

  defp run(conn, query, params) do
    with {:ok, matches} <- resolve_source(conn, query.source, deep?(query)) do
      if special_items?(query.select) do
        run_with_special_items(conn, query, matches, params)
      else
        run_flat_over_matches(matches, query, params)
      end
    end
  end

  # Neither a PARENT/SIBLINGS/ANCESTORS pseudo-field nor a nested SELECT
  # anywhere in this query's own top-level select -- nothing document-
  # specific to do. Delegating wholesale, unmodified query included, is
  # the correctness-critical path here: GROUP BY/aggregation only works
  # correctly when `run_flat/3` sees every row belonging to a group *at
  # once*, which the per-row marker technique `order_and_limit/3` needs
  # would never give it -- the identical reasoning `scry_document`'s own
  # reference `run/3` and `scry_engine_neo4j`'s own `run/3` both state.
  defp run_flat_over_matches(matches, query, params) do
    rows = Enum.map(matches, fn {_key, row} -> row end)
    QueryOps.run_flat(rows, query, params)
  end

  defp run_with_special_items(conn, query, matches, params) do
    with :ok <- validate_no_grouping(query),
         {:ok, ordered} <- order_and_limit(matches, query, params) do
      own_name = List.last(query.source)
      project_all(ordered, query.select, conn, own_name, params)
    end
  end

  defp special_items?(body_items) do
    Enum.any?(body_items, fn
      {:variant, {kind, _body}} when kind in [:parent, :siblings, :ancestors] -> true
      %Query{} -> true
      _other -> false
    end)
  end

  defp deep?(%Query{variant: %{select_ep1a: :deep}}), do: true
  defp deep?(_query), do: false

  defp validate_no_grouping(%Query{group_bys: []}), do: :ok
  defp validate_no_grouping(_query), do: {:error, {:unsupported, :pseudo_field_with_group_by}}

  defp resolve_source(conn, source, false) do
    with {:ok, rows} <- Conn.find(conn, Enum.join(source, ".")) do
      {:ok, Enum.map(rows, &{source, &1})}
    end
  end

  defp resolve_source(conn, source, true) do
    with {:ok, names} <- Conn.collection_names(conn) do
      keys =
        names
        |> Enum.map(&String.split(&1, "."))
        |> Enum.filter(&deep_match?(&1, source))
        |> Enum.sort()

      fetch_all(conn, keys)
    end
  end

  defp deep_match?(key, [only]), do: List.last(key) == only

  defp deep_match?(key, source),
    do: List.first(key) == List.first(source) and List.last(key) == List.last(source)

  defp fetch_all(conn, keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case Conn.find(conn, Enum.join(key, ".")) do
        {:ok, rows} -> {:cont, {:ok, acc ++ Enum.map(rows, &{key, &1})}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # Threads a unique, synthetic per-row index through `run_flat/3` (not
  # the tree key itself -- two distinct matched rows can legitimately
  # share the same key) so the post-filter/order/limit survivor list can
  # be mapped back to its own original `{key, row}` pair. Mirrors
  # `scry_document`'s reference `Executor`'s own identical technique.
  defp order_and_limit(matches, query, params) do
    indexed = Enum.with_index(matches)
    lookup = Map.new(indexed, fn {{key, row}, idx} -> {idx, {key, row}} end)

    tagged_rows =
      Enum.map(indexed, fn {{_key, row}, idx} -> Map.put(row, @document_key_field, idx) end)

    marker_query = %{query | select: [{:field, [@document_key_field]}]}

    with {:ok, marker_rows} <- QueryOps.run_flat(tagged_rows, marker_query, params) do
      ordered =
        marker_rows
        |> Enum.to_list()
        |> Enum.map(fn %{@document_key_field => idx} -> Map.fetch!(lookup, idx) end)

      {:ok, ordered}
    end
  end

  defp project_all(ordered, select, conn, own_name, params) do
    ordered
    |> Enum.map(fn {key, row} -> project_body(key, row, select, conn, own_name, params) end)
    |> Enum.split_with(&match?({:error, _}, &1))
    |> case do
      {[], oks} -> {:ok, Enum.map(oks, fn {:ok, row} -> row end)}
      {[first_error | _], _rows} -> first_error
    end
  end

  # Projects one already-resolved `{key, row}` against `body` -- plain
  # fields delegate to `Scry.Core.QueryOps.run_flat/3`, a nested `%Scry.
  # Core.Query{}` body item resolves via `resolve_correlated_nested/5`,
  # and `PARENT`/`SIBLINGS`/`ANCESTORS` resolve recursively through this
  # same function, one level relative to `key`. `own_name` is always the
  # *original, top-level* query's own source name, unchanged as this
  # recurses into a pseudo-field's own nested body -- the identical
  # scope limit `scry_document`'s reference `Executor` already states.
  defp project_body(key, row, body, conn, own_name, params) do
    pseudo_items = extract_pseudo_items(body)

    {nested_items, flat_select} =
      body |> strip_pseudo_items() |> Enum.split_with(&is_struct(&1, Query))

    with {:ok, base} <- project_ordinary(row, flat_select, params),
         {:ok, with_nested} <- add_nested_results(base, nested_items, row, conn, own_name, params) do
      resolve_pseudo_items(pseudo_items, with_nested, key, conn, own_name, params)
    end
  end

  defp resolve_pseudo_items([], base, _key, _conn, _own_name, _params), do: {:ok, base}

  defp resolve_pseudo_items(pseudo_items, base, key, conn, own_name, params) do
    Enum.reduce_while(pseudo_items, {:ok, base}, fn item, {:ok, acc} ->
      resolve_one_pseudo_item(item, acc, key, conn, own_name, params)
    end)
  end

  defp resolve_one_pseudo_item({output_key, kind, nested_body}, acc, key, conn, own_name, params) do
    case resolve_pseudo_field(kind, nested_body, key, conn, own_name, params) do
      {:ok, value} -> {:cont, {:ok, Map.put(acc, output_key, value)}}
      {:error, _} = err -> {:halt, err}
    end
  end

  defp extract_pseudo_items(body_items) do
    Enum.flat_map(body_items, fn
      {:variant, {kind, body}} when kind in [:parent, :siblings, :ancestors] ->
        [{Atom.to_string(kind), kind, body}]

      _other ->
        []
    end)
  end

  defp strip_pseudo_items(body_items) do
    Enum.reject(body_items, fn
      {:variant, {kind, _body}} when kind in [:parent, :siblings, :ancestors] -> true
      _other -> false
    end)
  end

  defp add_nested_results(base, [], _row, _conn, _own_name, _params), do: {:ok, base}

  defp add_nested_results(base, nested_items, row, conn, own_name, params) do
    Enum.reduce_while(nested_items, {:ok, base}, fn nested, {:ok, acc} ->
      resolve_nested(nested, acc, row, conn, own_name, params)
    end)
  end

  defp resolve_nested(nested, acc, row, conn, own_name, params) do
    fetch_fn = fn q, p -> fetch_and_drain(conn, q, p) end

    case QueryOps.resolve_correlated_nested(nested, row, own_name, params, fetch_fn) do
      {:ok, rows} -> {:cont, {:ok, Map.put(acc, List.last(nested.source), rows)}}
      {:error, _} = err -> {:halt, err}
    end
  end

  defp fetch_and_drain(conn, query, params) do
    with {:ok, enumerable} <- execute(conn, query, params) do
      {:ok, Enum.to_list(enumerable)}
    end
  end

  defp project_ordinary(_row, [], _params), do: {:ok, %{}}

  defp project_ordinary(row, select, params) do
    case QueryOps.run_flat([row], %Query{select: select}, params) do
      {:ok, enumerable} ->
        case Enum.to_list(enumerable) do
          [projected] -> {:ok, projected}
          [] -> {:ok, %{}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve_pseudo_field(:parent, body, key, conn, own_name, params) do
    case parent_key(key) do
      nil -> {:ok, nil}
      parent_key -> project_first(parent_key, body, conn, own_name, params)
    end
  end

  defp resolve_pseudo_field(:siblings, body, key, conn, own_name, params) do
    parent = parent_key(key)

    with {:ok, names} <- Conn.collection_names(conn) do
      names
      |> Enum.map(&String.split(&1, "."))
      |> Enum.filter(&(&1 != key and parent_key(&1) == parent))
      |> Enum.sort()
      |> project_all_rows(body, conn, own_name, params)
    end
  end

  defp resolve_pseudo_field(:ancestors, body, key, conn, own_name, params) do
    key
    |> ancestor_keys()
    |> Enum.reduce_while({:ok, []}, fn ancestor_key, {:ok, acc} ->
      case project_first(ancestor_key, body, conn, own_name, params) do
        {:ok, value} -> {:cont, {:ok, acc ++ [value]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp project_all_rows(keys, body, conn, own_name, params) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      fetch_and_project_rows(key, body, conn, own_name, params, acc)
    end)
  end

  defp fetch_and_project_rows(key, body, conn, own_name, params, acc) do
    with {:ok, rows} <- Conn.find(conn, Enum.join(key, ".")),
         {:ok, projected} <- project_rows(key, rows, body, conn, own_name, params) do
      {:cont, {:ok, acc ++ projected}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  defp project_rows(key, rows, body, conn, own_name, params) do
    rows
    |> Enum.map(fn row -> project_body(key, row, body, conn, own_name, params) end)
    |> Enum.split_with(&match?({:error, _}, &1))
    |> case do
      {[], oks} -> {:ok, Enum.map(oks, fn {:ok, r} -> r end)}
      {[first_error | _], _rows} -> first_error
    end
  end

  defp project_first(key, body, conn, own_name, params) do
    with {:ok, row} <- Conn.find_one(conn, Enum.join(key, ".")) do
      project_first_row(row, key, body, conn, own_name, params)
    end
  end

  defp project_first_row(nil, _key, _body, _conn, _own_name, _params), do: {:ok, nil}

  defp project_first_row(row, key, body, conn, own_name, params),
    do: project_body(key, row, body, conn, own_name, params)

  defp parent_key([_single]), do: nil
  defp parent_key(key), do: Enum.drop(key, -1)

  defp ancestor_keys(key) when length(key) <= 1, do: []
  defp ancestor_keys(key), do: for(i <- (length(key) - 1)..1//-1, do: Enum.take(key, i))

  @sample_size 100

  @doc """
  `Scry.Core.EngineBehaviour`'s optional `describe_source/2` callback --
  samples up to #{@sample_size} documents from `source`'s own real
  MongoDB collection (`source` is the collection name directly, dot-
  joined tree-key segments included -- the identical convention `query.
  source` already uses everywhere else in this package) and reports
  every field name observed, with a best-effort scalar type inferred
  from the *first* sampled value seen for it. `nullable: true`
  unconditionally, for the identical reason `scry_engine_elasticsearch`/
  `scry_engine_redisearch` both already report it: MongoDB has no
  required-field/schema-validator concept this package can generically
  introspect (a `$jsonSchema` validator can exist, but nothing requires
  one, and reading it would only describe what's *allowed*, not what's
  actually present on every real document) -- this is sampled,
  best-effort information, never something `execute/3`'s own contract
  relies on for pushdown safety, since this package pushes nothing down
  at all (this module's own moduledoc has the full reasoning).
  """
  @impl true
  @spec describe_source(Conn.t(), String.t()) ::
          {:ok, [Scry.Core.EngineBehaviour.introspected_field()]}
          | {:error, :not_found}
          | {:error, {:introspection_error, term()}}
  def describe_source(%Conn{} = conn, source) do
    case Conn.sample(conn, source, @sample_size) do
      {:ok, []} -> {:error, :not_found}
      {:ok, docs} -> {:ok, fields_from_sample(docs)}
      {:error, {:query_error, reason}} -> {:error, {:introspection_error, reason}}
    end
  end

  defp fields_from_sample(docs) do
    docs
    |> Enum.reduce(%{}, fn doc, acc ->
      Map.merge(acc, doc, fn _k, existing, _new -> existing end)
    end)
    |> Enum.map(fn {name, value} ->
      %{name: name, nullable: true, scalar: infer_scalar(value)}
    end)
  end

  defp infer_scalar(value) when is_binary(value), do: :string
  defp infer_scalar(value) when is_integer(value), do: :integer
  defp infer_scalar(value) when is_float(value), do: :float
  defp infer_scalar(value) when is_boolean(value), do: :boolean
  defp infer_scalar(value) when is_map(value) or is_list(value), do: :json
  defp infer_scalar(_other), do: :unknown
end
