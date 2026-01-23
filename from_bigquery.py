
from google.cloud import bigquery
from data_model.core import Database, Table, Column, ContainsTable, HasColumn, References
import pandas as pd

# connect to bigquery 
project_id = "ai-field-alex-g"
dataset_id = "demo_ecommerce"

# this works since google app default credentials are set
client = bigquery.Client(project=project_id)

# read information schema tables 

def get_database_info(client: bigquery.Client, project_id: str, dataset_id: str) -> pd.DataFrame:
    return client.query(f"""
SELECT 
    DISTINCT table_catalog,
    table_schema,
    option_value as description
FROM `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.TABLES tables
    LEFT JOIN `{project_id}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS schemata_options
    ON tables.table_schema = schemata_options.schema_name
WHERE option_name = 'description'
""").to_dataframe()

def get_table_info(client: bigquery.Client, project_id: str, dataset_id: str) -> pd.DataFrame:
    return client.query(f"""
SELECT 
    tables.table_catalog,
    tables.table_schema,
    tables.table_name,
    tables.table_type,
    tables.creation_time,
    tables.ddl,
    table_options.option_value as description
FROM `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.TABLES as tables
    LEFT JOIN `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.TABLE_OPTIONS as table_options
    ON tables.table_catalog = table_options.table_catalog
    AND tables.table_schema = table_options.table_schema
    AND tables.table_name = table_options.table_name
WHERE table_type = 'BASE TABLE'
ORDER BY table_name
""").to_dataframe()

def get_column_info(client: bigquery.Client, project_id: str, dataset_id: str) -> pd.DataFrame:

    def _is_pk(row: pd.Series) -> bool:
        return isinstance(row['constraint_name'], str) and row['constraint_name'].endswith('pk$')
    
    def _is_fk(row: pd.Series) -> bool:
        return isinstance(row['constraint_name'], str) and '.fk_' in row['constraint_name']

    df = client.query(f"""
SELECT 
    columns.table_catalog,
    columns.table_schema,
    columns.table_name,
    columns.column_name,
    columns.is_nullable,
    columns.data_type,
    column_field_paths.description,
    key_column_usage.constraint_name

FROM `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.COLUMNS as columns
    LEFT JOIN `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS column_field_paths
    ON columns.table_schema = column_field_paths.table_schema
    AND columns.table_name = column_field_paths.table_name
    AND columns.column_name = column_field_paths.column_name

    LEFT JOIN `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE key_column_usage
    ON columns.table_schema = key_column_usage.table_schema
    AND columns.table_name = key_column_usage.table_name
    AND columns.column_name = key_column_usage.column_name
""").to_dataframe()

    df['is_primary_key'] = df.apply(_is_pk, axis=1)
    df['is_foreign_key'] = df.apply(_is_fk, axis=1)
    return df

def get_column_references_info(client: bigquery.Client, project_id: str, dataset_id: str) -> pd.DataFrame:
    return client.query(f"""
SELECT 
    tc.constraint_catalog,
    tc.constraint_schema,
    tc.constraint_name,
    tc.constraint_type,
    tc.table_name,
    kcu.column_name,
    kcu.ordinal_position,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.constraint_name = kcu.constraint_name
LEFT JOIN `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
    ON tc.constraint_name = ccu.constraint_name
ORDER BY tc.table_name, tc.constraint_type, kcu.ordinal_position
""").to_dataframe()


def get_database_nodes(database_info: pd.DataFrame) -> list[Database]:
    return [Database(
        id=row.table_catalog + "." + row.table_schema,
        name=row.table_catalog + "." + row.table_schema,
        description=row.description
    ) for _, row in database_info.iterrows()]

def get_table_nodes(table_info: pd.DataFrame) -> list[Table]:
    return [Table(
        id=info_row.table_catalog + "." + info_row.table_schema + "." + info_row.table_name,
        name=info_row.table_name,
        description=info_row.description
    ) for _, info_row in table_info.iterrows()]

def get_column_nodes(column_info: pd.DataFrame) -> list[Column]:
    return [Column(
        id=row.table_catalog + "." + row.table_schema + "." + row.table_name + "." + row.column_name,
        name=row.column_name,
        description=row.description,
        type=row.data_type,
        nullable=row.is_nullable,
        is_primary_key=row.is_primary_key,
        is_foreign_key=row.is_foreign_key
    ) for _, row in column_info.iterrows()]

def get_contains_table_relationships(table_info: pd.DataFrame) -> list[ContainsTable]:
    return [ContainsTable(
        database_id=row.table_catalog + "." + row.table_schema,
        table_id=row.table_catalog + "." + row.table_schema + "." + row.table_name
    ) for _, row in table_info.iterrows()]

def get_has_column_relationships(column_info: pd.DataFrame) -> list[HasColumn]:
    return [HasColumn(
        table_id=row.table_catalog + "." + row.table_schema + "." + row.table_name,
        column_id=row.table_catalog + "." + row.table_schema + "." + row.table_name + "." + row.column_name
    ) for _, row in column_info.iterrows()]

def get_references_relationships(column_references_info: pd.DataFrame) -> list[References]:
    return [References(
        source_column_id=row.constraint_catalog + "." + row.constraint_schema + "." + row.table_name + "." + row.column_name,
        target_column_id=row.constraint_catalog + "." + row.constraint_schema + "." + row.referenced_table + "." + row.referenced_column
    ) for _, row in column_references_info[column_references_info['constraint_type'] == 'FOREIGN KEY'].iterrows()]
# convert to core data model

if __name__ == "__main__":
    database_info = get_database_info(client, project_id, dataset_id)
    table_info = get_table_info(client, project_id, dataset_id)
    column_info = get_column_info(client, project_id, dataset_id)
    column_references_info = get_column_references_info(client, project_id, dataset_id)

    database_nodes = get_database_nodes(database_info)
    table_nodes = get_table_nodes(table_info)
    column_nodes = get_column_nodes(column_info)

    contains_table_relationships = get_contains_table_relationships(table_info)
    has_column_relationships = get_has_column_relationships(column_info)
    references_relationships = get_references_relationships(column_references_info)

    print(database_nodes, '\n')
    print(table_nodes, '\n')
    print(column_nodes, '\n')

    print(contains_table_relationships, '\n')
    print(has_column_relationships, '\n')
    print(references_relationships, '\n')