defmodule Etso.Adapter.Behaviour.Schema do
  @moduledoc false
  @behaviour Ecto.Adapter.Schema

  alias Etso.ETS
  alias Etso.ETS.Table

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: :erlang.unique_integer()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()
  def autogenerate(:embed_id), do: Ecto.UUID.bingenerate()

  @impl Ecto.Adapter.Schema
  def insert_all(%{repo: repo}, %{schema: schema}, _, entries, _, _, _, _) do
    table = Table.build(repo, schema)
    result = ETS.insert_all(table, entries)

    case result do
      :ok -> {length(entries), []}
      :error -> {0, nil}
    end
  end

  @impl Ecto.Adapter.Schema
  def insert(%{repo: repo}, %{schema: schema}, fields, on_conflict, _, _) do
    table = Table.build(repo, schema)
    result = ETS.insert(table, fields)

    case result do
      :ok -> {:ok, []}
      :error -> handle_conflict(table, fields, on_conflict)
    end
  end

  @impl Ecto.Adapter.Schema
  def update(%{repo: repo}, %{schema: schema}, fields, filters, [], _) do
    table = Table.build(repo, schema)
    key = Keyword.fetch!(filters, table.primary_key)
    result = ETS.update(table, key, fields)

    case result do
      :ok -> {:ok, []}
      :error -> {:error, :stale}
    end
  end

  @impl Ecto.Adapter.Schema
  def delete(%{repo: repo}, %{schema: schema}, filters, _, _) do
    table = Table.build(repo, schema)
    key = Keyword.fetch!(filters, table.primary_key)
    ETS.delete(table, key)

    {:ok, []}
  end

  defp handle_conflict(_table, _fields, {:raise, _, _}) do
    {:invalid, [unique: "primary_key"]}
  end

  defp handle_conflict(_table, _fields, {:nothing, _, _}) do
    {:ok, []}
  end

  defp handle_conflict(_table, _fields, {%Ecto.Query{}, _, _}) do
    raise ArgumentError,
          "queries cannot be used to handle insert conflicts for ets tables"
  end

  defp handle_conflict(table, fields, {replace, _, [target]}) do
    unless target == table.primary_key do
      raise ArgumentError, "invalid field `#{inspect(target)}` in :conflict_target"
    end

    {key, fields} = Keyword.pop(fields, table.primary_key)
    {fields, _} = Keyword.split(fields, replace)
    result = ETS.update(table, key, fields)

    case result do
      :ok -> {:ok, []}
      :error -> {:error, :stale}
    end
  end
end
