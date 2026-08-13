defmodule Scry.Engine.MongoDB.Conn do
  @moduledoc """
  Wraps a `mongodb_driver` connection pid -- opened once via `open/1`
  and meant to be reused across many `Scry.Engine.MongoDB.execute/3`
  calls, matching the connection/config struct every real adapter
  exposes (impl_spec.md §2). `mongodb_driver` is `DBConnection`-based
  (`Mongo.start_link/1`), the same supervised-reconnecting-process shape
  `redix`/`myxql`/`postgrex`/`boltx` already have.

  Every read this package issues goes through `find/3`/`find_one/3`/
  `collection_names/1` here, never `Mongo.find/4` etc. directly, for two
  reasons: `_id` (a real `%BSON.ObjectId{}` struct `mongodb_driver`
  always attaches, confirmed directly) is stripped from every row before
  it reaches any of `Scry.Core.QueryOps`'s own generic evaluation --
  `Scry.Document.Conn`'s own reference rows have no implicit `"_id"`
  field at all (a plain user-supplied map, `Scry.Document.Conn`'s own
  moduledoc confirms), so leaving MongoDB's own driver-internal identity
  in a returned row would leak an implementation detail the reference
  model has no equivalent for, not real user data; and every `Mongo.*`
  call already returns/raises in a shape this package normalizes to an
  ordinary `{:ok, _} | {:error, {:query_error, _}}` two-tuple, the one
  shape every caller in this package needs to handle.
  """

  @type t :: %__MODULE__{pid: pid()}

  defstruct pid: nil

  @default_opts [url: "mongodb://localhost:27017/scry"]

  @doc """
  Starts a `mongodb_driver` connection against `opts`
  (`Mongo.start_link/1`'s own options), merged over this module's own
  explicit local-Docker default (`url: "mongodb://localhost:27017/scry"`).
  """
  @spec open(keyword()) :: {:ok, t()} | {:error, term()}
  def open(opts \\ []) do
    with {:ok, pid} <- Mongo.start_link(Keyword.merge(@default_opts, opts)) do
      {:ok, %__MODULE__{pid: pid}}
    end
  end

  @doc "Stops the wrapped connection."
  @spec close(t()) :: :ok
  def close(%__MODULE__{pid: pid}), do: GenServer.stop(pid)

  @doc "Every document in `collection`, `_id` stripped, unfiltered -- ordinary `WHERE`/etc. apply generically afterward, never pushed down here (this package's own moduledoc has the full reasoning)."
  @spec find(t(), String.t()) :: {:ok, [map()]} | {:error, {:query_error, term()}}
  def find(%__MODULE__{pid: pid}, collection) when is_binary(collection) do
    {:ok, pid |> Mongo.find(collection, %{}) |> Enum.map(&strip_id/1) |> Enum.to_list()}
  rescue
    e -> {:error, {:query_error, Exception.message(e)}}
  end

  @doc "The first document in `collection` (`_id` stripped), or `nil` if it's empty or doesn't exist -- `PARENT`/`ANCESTORS`'s own \"first row only\" simplification (`Scry.Document.Executor`'s own moduledoc), mirrored here."
  @spec find_one(t(), String.t()) :: {:ok, map() | nil} | {:error, {:query_error, term()}}
  def find_one(%__MODULE__{pid: pid}, collection) when is_binary(collection) do
    case Mongo.find_one(pid, collection, %{}) do
      nil -> {:ok, nil}
      doc -> {:ok, strip_id(doc)}
    end
  rescue
    e -> {:error, {:query_error, Exception.message(e)}}
  end

  @doc "Every real collection name in the connected database, `system.*` collections excluded -- `DEEP`'s own cross-collection matching, and `PARENT`/`SIBLINGS`/`ANCESTORS`'s own relative-collection resolution, both need the full set to filter against."
  @spec collection_names(t()) :: {:ok, [String.t()]} | {:error, {:query_error, term()}}
  def collection_names(%__MODULE__{pid: pid}) do
    names =
      pid
      |> Mongo.show_collections()
      |> Enum.to_list()
      |> Enum.reject(&String.starts_with?(&1, "system."))

    {:ok, names}
  rescue
    e -> {:error, {:query_error, Exception.message(e)}}
  end

  @doc "Up to `limit` documents from `collection` (`_id` stripped), for best-effort field/type introspection -- never used by `execute/3`'s own contract."
  @spec sample(t(), String.t(), pos_integer()) ::
          {:ok, [map()]} | {:error, {:query_error, term()}}
  def sample(%__MODULE__{pid: pid}, collection, limit) when is_binary(collection) do
    docs =
      pid |> Mongo.find(collection, %{}, limit: limit) |> Enum.map(&strip_id/1) |> Enum.to_list()

    {:ok, docs}
  rescue
    e -> {:error, {:query_error, Exception.message(e)}}
  end

  defp strip_id(doc), do: Map.delete(doc, "_id")
end
