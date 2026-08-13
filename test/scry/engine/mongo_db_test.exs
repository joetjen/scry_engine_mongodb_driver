defmodule Scry.Engine.MongoDBTest do
  @moduledoc """
  `Scry.Engine.MongoDB` -- confirms `execute/3` answers an ordinary flat
  query (no `DEEP`/`PARENT`/`SIBLINGS`/`ANCESTORS`) entirely via `Scry.
  Core.QueryOps.run_flat/3` over a real MongoDB collection's own
  documents, that `DEEP` matches across every real collection sharing
  the right first/last name segment, that `PARENT`/`SIBLINGS`/
  `ANCESTORS` resolve against real sibling/ancestor collections
  (`Scry.Engine.MongoDB.Conn.collection_names/1`-backed), that nesting
  one pseudo-field inside another wraps rather than flattening, that a
  pseudo-field/nested `SELECT` combined with `GROUP BY` declines, that
  an ordinary `GROUP BY` with no pseudo items still aggregates
  correctly, and that `%Scry.Core.CombinedQuery{}`/a `WITH`-bound source
  both resolve via `Scry.Core.QueryOps.run_document/4` -- all against a
  real `mongo:7` container, not just plausible-looking output.

  **Requires a real, reachable MongoDB instance** -- run one locally via
  `docker run -d --name scry-mongo -p 27017:27017 mongo:7`. Runs
  `async: false` -- every test shares one real server and a small,
  fixed set of collections, torn down and rebuilt in `setup_all`.
  """

  use ExUnit.Case, async: false

  alias Scry.Core.{CombinedQuery, Query}
  alias Scry.Engine.MongoDB, as: Engine
  alias Scry.Engine.MongoDB.Conn

  setup_all do
    {:ok, conn} = Conn.open()
    seed!(conn)
    %{conn: conn}
  end

  defp seed!(conn) do
    Mongo.command!(conn.pid, dropDatabase: 1)

    Mongo.insert_one!(conn.pid, "library", %{"id" => 1, "name" => "Main", "region" => "north"})
    Mongo.insert_one!(conn.pid, "library.fiction", %{"name" => "Fiction"})
    Mongo.insert_one!(conn.pid, "library.nonfiction", %{"name" => "Non-Fiction"})

    Mongo.insert_many!(conn.pid, "library.fiction.book", [
      %{"id" => 1, "title" => "Dune", "year" => 1965},
      %{"id" => 2, "title" => "Foundation", "year" => 1951}
    ])

    Mongo.insert_one!(conn.pid, "other.book", %{"title" => "Wrong Root"})
    # Namespaced under "app" rather than top-level, so these don't leak
    # into "library"'s own SIBLINGS set below (a top-level collection's
    # siblings are every *other* top-level collection -- a real
    # consequence of one shared fixture across every describe block
    # here, not a special case this package's own SIBLINGS resolution
    # needs to guard against).
    Mongo.insert_one!(conn.pid, "app.notes", %{"library_id" => 1, "text" => "important"})
    Mongo.insert_one!(conn.pid, "app.reviews", %{"book_id" => 1, "stars" => 5})
  end

  defp materialize({:ok, rows}), do: {:ok, rows |> Enum.to_list()}
  defp materialize(other), do: other

  describe "no DEEP -- exact collection match, delegated to Scry.Core.QueryOps.run_flat/3" do
    test "matches only the literal collection", %{conn: conn} do
      query = %Query{source: ["library"], select: [{:field, ["name"]}]}
      assert {:ok, [%{"name" => "Main"}]} = materialize(Engine.execute(conn, query, %{}))
    end

    test "a collection matching no document at all is an empty result, not an error", %{
      conn: conn
    } do
      query = %Query{source: ["nonexistent"], select: [{:field, ["name"]}]}
      assert {:ok, []} = materialize(Engine.execute(conn, query, %{}))
    end

    test "ordinary WHERE/ORDER BY/LIMIT still work", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :gt, ["year"], 1960}],
        select: [{:field, ["title"]}]
      }

      assert {:ok, [%{"title" => "Dune"}]} = materialize(Engine.execute(conn, query, %{}))
    end

    test "GROUP BY/aggregate works generically", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction", "book"],
        select: [{:computed, "total", {:call, "count", [{:field, ["title"]}]}}]
      }

      assert {:ok, [%{"total" => 2}]} = materialize(Engine.execute(conn, query, %{}))
    end
  end

  describe "DEEP -- matches across every real collection sharing first/last name segments" do
    test "matches both a direct child and a deeply-nested descendant", %{conn: conn} do
      query = %Query{
        source: ["library", "book"],
        variant: %{select_ep1a: :deep},
        select: [{:field, ["title"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      assert Enum.map(rows, & &1["title"]) |> Enum.sort() == ["Dune", "Foundation"]
    end

    test "never matches a collection with a different first segment", %{conn: conn} do
      query = %Query{
        source: ["library", "book"],
        variant: %{select_ep1a: :deep},
        select: [{:field, ["title"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      refute "Wrong Root" in Enum.map(rows, & &1["title"])
    end

    test "a single-segment source only constrains the last segment", %{conn: conn} do
      query = %Query{
        source: ["book"],
        variant: %{select_ep1a: :deep},
        select: [{:field, ["title"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      assert Enum.map(rows, & &1["title"]) |> Enum.sort() == ["Dune", "Foundation", "Wrong Root"]
    end

    test "without DEEP, the same multi-segment source only matches the literal collection", %{
      conn: conn
    } do
      query = %Query{source: ["library", "book"], select: [{:field, ["title"]}]}
      assert {:ok, []} = materialize(Engine.execute(conn, query, %{}))
    end
  end

  describe "PARENT/SIBLINGS/ANCESTORS" do
    test "PARENT resolves to the row one level up, projected through its own body", %{
      conn: conn
    } do
      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :eq, ["title"], "Dune"}],
        select: [{:field, ["title"]}, {:variant, {:parent, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"title" => "Dune", "parent" => %{"name" => "Fiction"}}
    end

    test "PARENT is nil at the root", %{conn: conn} do
      query = %Query{
        source: ["library"],
        select: [{:field, ["name"]}, {:variant, {:parent, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"name" => "Main", "parent" => nil}
    end

    test "SIBLINGS resolves every row in every sibling collection, excluding the row's own collection",
         %{conn: conn} do
      query = %Query{
        source: ["library"],
        select: [{:field, ["name"]}, {:variant, {:siblings, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"name" => "Main", "siblings" => []}

      query2 = %Query{
        source: ["library", "fiction"],
        select: [{:field, ["name"]}, {:variant, {:siblings, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row2]} = materialize(Engine.execute(conn, query2, %{}))
      assert row2 == %{"name" => "Fiction", "siblings" => [%{"name" => "Non-Fiction"}]}
    end

    test "ANCESTORS returns one row per level, nearest first, root last", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :eq, ["title"], "Dune"}],
        select: [{:field, ["title"]}, {:variant, {:ancestors, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row["ancestors"] == [%{"name" => "Fiction"}, %{"name" => "Main"}]
    end

    test "ANCESTORS is an empty list at the root", %{conn: conn} do
      query = %Query{
        source: ["library"],
        select: [{:field, ["name"]}, {:variant, {:ancestors, [{:field, ["name"]}]}}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"name" => "Main", "ancestors" => []}
    end

    test "nesting PARENT inside PARENT wraps rather than flattening", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :eq, ["title"], "Dune"}],
        select: [
          {:variant, {:parent, [{:variant, {:parent, [{:field, ["name"]}]}}]}}
        ]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"parent" => %{"parent" => %{"name" => "Main"}}}
    end

    test "PARENT/SIBLINGS/ANCESTORS together in one query", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction"],
        select: [
          {:field, ["name"]},
          {:variant, {:parent, [{:field, ["name"]}]}},
          {:variant, {:siblings, [{:field, ["name"]}]}},
          {:variant, {:ancestors, [{:field, ["name"]}]}}
        ]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))

      assert row == %{
               "name" => "Fiction",
               "parent" => %{"name" => "Main"},
               "siblings" => [%{"name" => "Non-Fiction"}],
               "ancestors" => [%{"name" => "Main"}]
             }
    end
  end

  describe "a nested correlated SELECT sibling of an ordinary field" do
    test "correlates to the top-level source's own row", %{conn: conn} do
      nested = %Query{
        source: ["app", "notes"],
        wheres: [{:cmp, :eq, ["library_id"], {:field, ["library", "id"]}}],
        select: [{:field, ["text"]}]
      }

      query = %Query{
        source: ["library"],
        select: [{:field, ["name"]}, nested]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row["name"] == "Main"
      assert row["notes"] == [%{"text" => "important"}]
    end

    test "composes correctly alongside PARENT, in the same body", %{conn: conn} do
      nested = %Query{
        source: ["app", "reviews"],
        wheres: [{:cmp, :eq, ["book_id"], {:field, ["book", "id"]}}],
        select: [{:field, ["stars"]}]
      }

      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :eq, ["title"], "Dune"}],
        select: [
          {:field, ["title"]},
          {:variant, {:parent, [{:field, ["name"]}]}},
          nested
        ]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))

      assert row == %{
               "title" => "Dune",
               "parent" => %{"name" => "Fiction"},
               "reviews" => [%{"stars" => 5}]
             }
    end

    test "an unmatched correlation yields an empty nested list, not an error", %{conn: conn} do
      nested = %Query{
        source: ["app", "reviews"],
        wheres: [{:cmp, :eq, ["book_id"], {:field, ["book", "id"]}}],
        select: [{:field, ["stars"]}]
      }

      query = %Query{
        source: ["library", "fiction", "book"],
        wheres: [{:cmp, :eq, ["title"], "Foundation"}],
        select: [{:field, ["title"]}, nested]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"title" => "Foundation", "reviews" => []}
    end
  end

  describe "GROUP BY scope limits" do
    test "a pseudo-field alongside GROUP BY declines explicitly", %{conn: conn} do
      query = %Query{
        source: ["library", "fiction", "book"],
        group_bys: [["year"]],
        select: [{:field, ["year"]}, {:variant, {:parent, [{:field, ["name"]}]}}]
      }

      assert {:error, {:unsupported, :pseudo_field_with_group_by}} =
               materialize(Engine.execute(conn, query, %{}))
    end

    test "an ordinary GROUP BY with no pseudo items at all still aggregates correctly", %{
      conn: conn
    } do
      query = %Query{
        source: ["library", "fiction", "book"],
        select: [{:computed, "total", {:call, "count", [{:field, ["title"]}]}}]
      }

      assert {:ok, [%{"total" => 2}]} = materialize(Engine.execute(conn, query, %{}))
    end
  end

  describe "%Scry.Core.CombinedQuery{} and a WITH-bound source" do
    test "CombinedQuery delegates to Scry.Core.QueryOps.run_document/4", %{conn: conn} do
      left = %Query{source: ["library", "fiction"], select: [{:field, ["name"]}]}
      right = %Query{source: ["library", "nonfiction"], select: [{:field, ["name"]}]}
      combined = %CombinedQuery{op: :union, left: left, right: right}

      assert {:ok, rows} = materialize(Engine.execute(conn, combined, %{}))
      assert rows |> Enum.map(& &1["name"]) |> Enum.sort() == ["Fiction", "Non-Fiction"]
    end

    test "a WITH-bound top-level source runs the binding instead of a real collection", %{
      conn: conn
    } do
      binding = %Query{source: ["library"], select: [{:field, ["name"]}]}

      query = %Query{
        source: ["main_only"],
        with_bindings: %{"main_only" => binding},
        select: [{:field, ["name"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      assert Enum.map(rows, & &1["name"]) == ["Main"]
    end
  end

  describe "describe_source/2" do
    test "reports every observed field on the given collection", %{conn: conn} do
      assert {:ok, fields} = Engine.describe_source(conn, "library.fiction.book")
      by_name = Map.new(fields, &{&1.name, &1})

      assert by_name["title"].scalar == :string
      assert by_name["year"].scalar == :integer
      assert by_name["title"].nullable == true
    end

    test "a collection with no observed documents at all is not found", %{conn: conn} do
      assert {:error, :not_found} = Engine.describe_source(conn, "no_such_collection")
    end
  end
end
