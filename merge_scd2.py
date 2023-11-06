def merge_scd2(
        spark,
        source_catalog_name: str,
        target_catalog_name: str,
        table_name: str,
        source_suffix: str,  # using _updates for this
        target_suffix: str,  # using _history for this
        source_name: str,  # translates to the schema in Unity Catalog
        environment: str,  # forms part of the table name within the silver catalog
        data_run_date: str,  # the timestamp of the window end
        effective_from: str,  # the timestamp of the window start or 1970-01-01 for bulk load
        deletes: str = None
) -> None:
    """
    This function dynamically generates MERGE code that handles SCD Type 2 changes.
    It also can identify deletes in the source when deletes=True is passed.
    
    The function assumes it is implemented within a Unity Catalog workspace in Databricks,
    but can be adapted to any technology.
    
    The function expects a sorted list of columns from the source system (which in this implementation is
    obtained from the Bronze INFORMATION_SCHEMA).
    
    The function also expects a list of columns that are the primary keys of the table (in this
    implementation it is obtained from a query to the source database metadata table.

    The default columns variable is hard-coded based on columns that are added to the Bronze table during the
    load from the landing zone.  The _rescued_data column is an automatic column added by Auto Loader in
    case of data type mismatches.

    The scd columns variable is hard-coded (as are the values) with the standard tracking columns needed for
    SCD Type 2.

    The row_hash column is a MD5 digest of all the source columns.

    There is no output, the function executes a Spark SQL command to run the MERGE statement.

    However, the SQL can be output to be executed by some other function (e.g. via an ODBC connection).
    """
    
    source_table_name = f"{source_catalog_name}.{source_name}.{environment}_{table_name}_{source_suffix}"
    target_table_name = f"{target_catalog_name}.{source_name}.{environment}_{table_name}_{target_suffix}"
    # Default Columns block - these are set in the DLT pipeline
    default_columns = ['row_hash', '_rescued_data', 'file_name', 'source_table', 'source_refresh_freq_mins',
                       'source_data_as_at']
    default_sql = ', '.join([f"src.{i}" for i in default_columns])
    # SCD Tracking Columns block - these are standard defaults and are added to the source temporary table
    # ('src') as part of the merge
    scd_columns = ['effective_from', 'effective_to', 'is_current', 'is_deleted', 'modified_at']
    scd_values = [effective_from, '9999-12-31', '1', '0', str(datetime.datetime.now())]
    scd_columns_sql = ', '.join([f"{i}" for i in scd_columns])
    scd_values_sql = ', '.join([f"'{i}'" for i in scd_values])
    # Source Table Columns Block - these are the sorted source columns based on the table in the bronze
    # schema created in the DLT pipeline
    # get_sorted_table_columns queries the Bronze equivalent of the table to replicate the column order
    table_columns = [c.lower() for c in get_sorted_table_columns(spark, source_name, environment, table_name)]
    src_table_columns_sql = ', '.join([f"src.{i.lower()}" for i in table_columns])
    # Key Columns Block - these are obtained from the source tables configuration
    # config data is loaded in yaml files and converted to dicts then transformed into config catalog tables
    config_dict = load_config_data(spark, source_name, environment, data_run_date)
    tables_config = build_tables_config(config_dict['tables'])
    # this detail is obtained dynamically from the source database (but can also be loaded in a yaml file)
    key_columns = tables_config[table_name]['key_column']
    if type(key_columns) != list:
        key_columns = [key_columns]
    key_block = [f" src.{c} = tgt.{c} " for c in key_columns]
    # Insert Filter Columns Block - this is where the target columns are NULL in the full join
    insert_block = [f" tgt.{c} is null " for c in key_columns]
    # Insert Columns Block
    insert_columns = [*default_columns, *scd_columns, *table_columns]
    # Insert Values Block
    insert_values = f"{default_sql}, {scd_values_sql}, {src_table_columns_sql}"
    # Deleted Records Filter Block
    if deletes:
        filter_block = " tgt.is_current = 1 and tgt.is_deleted = 0 "
    else:
        filter_block = " tgt.is_current = 1 "
    sql = f"""
        MERGE INTO {target_table_name} as tgt USING (
        SELECT
            'insert' as action -- this is to insert newly created records (where keys do not exist in the target)
            ,{default_sql}
            ,{scd_columns_sql}
            ,{src_table_columns_sql}
        FROM
            {source_table_name} src
        LEFT JOIN {target_table_name} as tgt ON
            {' and '.join(key_block)}
            and {filter_block}
        WHERE
            {' and '.join(insert_block)}
        UNION ALL
        SELECT
            'update' as action -- this is to insert new records for changed records (where the row hash has changed)
            ,{default_sql}
            ,{scd_columns_sql}
            ,{src_table_columns_sql}
        FROM
            {source_table_name} src
        FULL JOIN {target_table_name} as tgt ON
            {' and '.join(key_block)}
        WHERE
            {filter_block}
            and src.row_hash <> tgt.row_hash
        """
    if deletes:
        sql += f"""
            UNION ALL
            SELECT
                'unchanged' as action -- this is to ensure the WHEN NOT MATCHED clause works as expected (where the row hash is unchanged)
                ,{default_sql}
                ,{scd_columns_sql}
                ,{src_table_columns_sql}
            FROM
                {source_table_name} src
            FULL JOIN {target_table_name} as tgt ON
                {' and '.join(key_block)}
            WHERE
                {filter_block}
                and src.row_hash = tgt.row_hash
        """
    sql += f"""
        ) src
        ON
            {' and '.join(key_block)}
            and {filter_block}
        WHEN MATCHED AND src.action = 'update' THEN
        UPDATE SET
            is_current = 0,
            effective_to = ('{effective_from}' - interval 1000 microsecond)
        WHEN NOT MATCHED THEN
            INSERT({','.join(insert_columns)})
            VALUES({insert_values})
    """
    if deletes:
        sql += f"""
            WHEN NOT MATCHED BY SOURCE AND is_deleted = 0 THEN
            UPDATE SET
                is_current = 0,
                is_deleted = 1,
                effective_to = ('{data_run_date}' - interval 1000 microsecond)
        """
    spark.sql(sql)
