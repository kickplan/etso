defmodule Etso.ETS do
  @moduledoc false

  import Kernel, except: [apply: 2]

  alias Etso.ETS.Table

  def insert(%Table{} = table, data) do
    changes = changeset(table, {:insert, data})

    apply(:insert_new, [table.reference, changes])
  end

  def insert_all(%Table{} = table, data) do
    changes = changeset(table, {:insert_all, data})

    apply(:insert_new, [table.reference, changes])
  end

  def delete(%Table{} = table, key) do
    apply(:delete, [table.reference, key])
  end

  def update(%Table{} = table, key, data) do
    changes = changeset(table, {:update, data})

    apply(:update_element, [table.reference, key, changes])
  end

  defp apply(fun, args) do
    if apply(:ets, fun, args), do: :ok, else: :error
  end

  defp changeset(table, {:insert, data}) do
    table.fields
    |> Keyword.keys()
    |> Enum.map(&Keyword.get(data, &1, nil))
    |> List.to_tuple()
  end

  defp changeset(table, {:insert_all, data}) do
    for entry <- data do
      changeset(table, {:insert, entry})
    end
  end

  defp changeset(table, {:update, data}) do
    for {name, value} <- data do
      {table.fields[name], value}
    end
  end
end
