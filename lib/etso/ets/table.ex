defmodule Etso.ETS.Table do
  @moduledoc false

  alias Etso.Adapter.TableRegistry

  defstruct [:fields, :primary_key, :reference]

  def build(repo, schema) do
    %__MODULE__{}
    |> set_fields(schema)
    |> set_primary_key(schema)
    |> set_reference(repo, schema)
  end

  def field_names(schema) do
    fields = schema.__schema__(:fields)
    primary_key = schema.__schema__(:primary_key)
    primary_key ++ (fields -- primary_key)
  end

  defp set_fields(table, schema) do
    fields = field_names(schema) |> Enum.with_index(1)
    %{table | fields: fields}
  end

  defp set_primary_key(table, schema) do
    [primary_key] = schema.__schema__(:primary_key)
    %{table | primary_key: primary_key}
  end

  defp set_reference(table, repo, schema) do
    {:ok, reference} = TableRegistry.get_table(repo, schema)
    %{table | reference: reference}
  end
end
