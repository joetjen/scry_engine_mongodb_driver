defmodule Scry.Engine.MongoDB.ParityTest do
  @moduledoc """
  AGENTS.md's "Parity between multiple implementations" rule, applied
  directly: `scry_document`'s own reference `Scry.Document.Executor` (a
  plain in-memory `%{path => rows}` map) and this package's `Scry.
  Engine.MongoDB` (a real MongoDB-backed adapter) are two
  implementations of the identical `DEEP`/`PARENT`/`SIBLINGS`/
  `ANCESTORS` semantics (lang_spec.md §8.3) -- the same posture already
  established for `scry_graph`/`scry_engine_neo4j`. Rather than
  asserting each package's own output looks plausible in isolation,
  this suite parses one query text *once* (`Scry.Document.parse/1`, the
  same grammar/AST both engines are handed), then runs the exact same
  `%Scry.Core.Query{}` against a byte-for-byte identical fixture in
  both a real MongoDB container and the reference's own in-memory
  `Scry.Document.Conn`, and asserts the results agree.
  """

  use ExUnit.Case, async: false

  alias Scry.Core.Cursor
  alias Scry.Document.Conn, as: RefConn
  alias Scry.Document.Executor, as: RefEngine
  alias Scry.Engine.MongoDB, as: RealEngine
  alias Scry.Engine.MongoDB.Conn, as: RealConn

  setup_all do
    {:ok, real_conn} = RealConn.open()
    seed_real!(real_conn)
    %{real_conn: real_conn, ref_conn: fixture_ref_conn()}
  end

  # The identical fixture `scry_document`'s own `Scry.Document.
  # ExecutorTest`'s "PARENT/SIBLINGS/ANCESTORS" describe block uses,
  # node for node.
  defp fixture_ref_conn do
    RefConn.new(%{
      ["library"] => [%{"name" => "Main", "region" => "north"}],
      ["library", "fiction"] => [%{"name" => "Fiction"}],
      ["library", "nonfiction"] => [%{"name" => "Non-Fiction"}],
      ["library", "fiction", "book"] => [
        %{"title" => "Dune"},
        %{"title" => "Foundation"}
      ]
    })
  end

  defp seed_real!(conn) do
    Mongo.command!(conn.pid, dropDatabase: 1)

    Mongo.insert_one!(conn.pid, "library", %{"name" => "Main", "region" => "north"})
    Mongo.insert_one!(conn.pid, "library.fiction", %{"name" => "Fiction"})
    Mongo.insert_one!(conn.pid, "library.nonfiction", %{"name" => "Non-Fiction"})

    Mongo.insert_many!(conn.pid, "library.fiction.book", [
      %{"title" => "Dune"},
      %{"title" => "Foundation"}
    ])
  end

  defp run_both(source, ref_conn, real_conn) do
    {:ok, query} = Scry.Document.parse(source)
    {:ok, ref_cursor} = RefEngine.run(query, ref_conn)
    {:ok, real_enumerable} = RealEngine.execute(real_conn, query, %{})
    {Cursor.to_list(ref_cursor), Enum.to_list(real_enumerable)}
  end

  for {label, query_text} <- [
        {"no DEEP, exact key match", ~s(SELECT library.fiction.book { title })},
        {"DEEP, single-segment source", ~s(SELECT book DEEP { title })},
        {"PARENT resolves one level up",
         ~s(SELECT library.fiction.book { title, PARENT { name } })},
        {"PARENT is nil at the root", ~s(SELECT library { name, PARENT { name } })},
        {"SIBLINGS resolves the sibling collection's own rows",
         ~s(SELECT library.fiction { name, SIBLINGS { name } })},
        {"ANCESTORS returns one row per level, nearest first",
         ~s(SELECT library.fiction.book WHERE title = "Dune" { title, ANCESTORS { name } })},
        {"nesting PARENT inside PARENT wraps rather than flattening",
         ~s(SELECT library.fiction.book WHERE title = "Dune" { PARENT { PARENT { name } } })},
        {"PARENT/SIBLINGS/ANCESTORS together in one query",
         ~s(SELECT library.fiction { name, PARENT { name }, SIBLINGS { name }, ANCESTORS { name } })},
        {"ordinary WHERE/ORDER BY/LIMIT, no pseudo-field at all",
         ~s(SELECT library.fiction.book ORDER BY title LIMIT 1 { title })}
      ] do
    test "#{label} -- reference and real engine agree", %{
      real_conn: real_conn,
      ref_conn: ref_conn
    } do
      {ref_rows, real_rows} = run_both(unquote(query_text), ref_conn, real_conn)
      assert ref_rows == real_rows
    end
  end
end
