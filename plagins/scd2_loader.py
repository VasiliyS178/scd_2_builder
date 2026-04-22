import urllib3
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SCD2Loader(BaseOperator):
    def __init__(
        self,
        trg_table,
        src_table,
        key_columns,
        eff_from,
        wrk_schema,
        conn_id,
        reload_flg,
        partition_column,
        custom_dt,
        *args,
        **kwargs
    ) -> None:
        self.trg_table = trg_table
        self.src_table = src_table
        self.key_columns = key_columns
        self.eff_from = eff_from
        self.wrk_schema = wrk_schema
        self.reload_flg = reload_flg
        self.partition_column = partition_column
        self.custom_dt = custom_dt
        self.conn_id = conn_id

        super().__init__(*args, **kwargs)

    def run_query(self, query):
        connection = BaseHook.get_connection(conn_id=self.conn_id)
        if connection and connection.conn_type == 'trino':
            hook = connection.get_hook()
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            cur.close()
            conn.close()
        else:
            raise Exception(f"Unsupported connection type: {connection.conn_type}")
        return results

    def get_cols_array_without(self, table_catalog, table_schema, table_name, except_column):
        print("Start get_cols_array_without")

        if not table_schema or not table_name:
            raise ValueError("ERROR: Not enough passed arguments. Macros arguments: table_schema, table_name")

        print(f"Excluded Columns: {except_column}")

        query = f"""
            SELECT column_name 
            FROM {table_catalog}.information_schema.columns
            WHERE lower(table_schema) = '{table_schema}' 
            AND lower(table_name) = '{table_name}'
            AND column_name NOT IN ({', '.join([f"'{name}'" for name in except_column])})   
        """

        results = self.run_query(query)
        columns_array = [row[0] for row in results]

        return columns_array

    def check_tab_existence(self, table_catalog, table_schema, table_name):
        # Проверка переданных аргументов
        if not table_schema or not table_name:
            raise ValueError("ERROR: Not enough passed arguments. Macros arguments: table_schema, table_name")

        query = f"""
            SELECT count(1)
            FROM {table_catalog}.information_schema.columns
            WHERE lower(table_schema) = '{table_schema}' 
            AND lower(table_name) = '{table_name}'       
        """
        results = self.run_query(query)[0][0]
        if results > 0:
            return True
        return False

    def compare_lists(self, list1, list2):
        set1 = set(list1)
        set2 = set(list2)
        are_equal = set1 == set2
        return are_equal

    def generate_sql(self):
        trg_table_catalog, trg_table_schema, trg_table_name = self.trg_table.lower().split('.')
        if not self.check_tab_existence(trg_table_catalog, trg_table_schema, trg_table_name):
            raise ValueError('ERROR: Table ' + self.trg_table + ' does not exist')

        # Получение списка столбцов для таблицы-источника
        src_table_catalog, src_table_schema, src_table_name = self.src_table.lower().split('.')

        if not self.check_tab_existence(src_table_catalog, src_table_schema, src_table_name):
            raise ValueError('ERROR: Table ' + self.src_table + ' does not exist')

        extra_src_columns = [self.eff_from] if self.eff_from == 'dataflow_dttm' else [self.eff_from, 'dataflow_dttm']

        src_columns = self.get_cols_array_without(src_table_catalog, src_table_schema, src_table_name, extra_src_columns)
        tgt_columns = self.get_cols_array_without(
            trg_table_catalog, trg_table_schema, trg_table_name, ['valid_from_dttm', 'valid_to_dttm', 'dataflow_dttm']
        )

        if not self.compare_lists(src_columns, tgt_columns):
            raise ValueError('ERROR Src column names not equal target column names ')

        key_columns_arr = self.key_columns.split(',')
        src_columns_wo_key = list(set(src_columns) - set(key_columns_arr))

        # Create join condition
        count_keys = len(key_columns_arr)
        join_cond = ''
        for i in range(count_keys):
            if i < count_keys - 1:
                join_cond += f" t1.{key_columns_arr[i]} = t2.{key_columns_arr[i]} AND"
            else:
                join_cond += f" t1.{key_columns_arr[i]} = t2.{key_columns_arr[i]}"
        print(f'self.reload_flg: {self.reload_flg}')

        # Create filter for sliced table
        slice_filter = f"AND {self.partition_column} = CAST('{self.custom_dt}' AS DATE)" if self.reload_flg == '1' else ""

        # Drop temporary tables
        query_drop_tmp = f"""
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.current_slice;
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.previous_slice;                       
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.appended_rows;                       
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.deleted_rows;                       
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.updated_rows;                       
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.closed_rows;                       
            DROP TABLE IF EXISTS {trg_table_catalog}.{self.wrk_schema}.load_batch                       
        """

        current_slice = f"""
            CREATE TABLE {src_table_catalog}.{self.wrk_schema}.current_slice AS
            SELECT	
                {', '.join(src_columns)}, 
                LOWER(
                    TO_HEX(
                        MD5(
                            TO_UTF8(
                                CONCAT_WS(
                                    '|',                        
                                    {', '.join([f"COALESCE(CAST({col} AS VARCHAR), '#')" for col in src_columns_wo_key])}                                                       
                                )
                            )
                        )
                    )
                ) AS hashdiff_key,
                CAST(CAST({self.eff_from} AS DATE) AS TIMESTAMP) AS valid_from_dttm,
                CAST('5999-01-01 00:00:00' AS TIMESTAMP) AS valid_to_dttm
            FROM
                {self.src_table}
            WHERE
                1 = 1
                {slice_filter}              
        """

        previous_slice = f"""
            CREATE TABLE {trg_table_catalog}.{self.wrk_schema}.previous_slice AS 
            SELECT
                {', '.join(src_columns)},     
                LOWER(
                    TO_HEX(
                        MD5(
                            TO_UTF8(
                                CONCAT_WS(
                                    '|',                        
                                    {', '.join([f"COALESCE(CAST({col} AS VARCHAR), '#')" for col in src_columns_wo_key])}                      
                                )
                            )
                        )
                    )
                ) AS hashdiff_key,
                valid_from_dttm,
                valid_to_dttm
            FROM
                {self.trg_table}
            WHERE
                valid_to_dttm = CAST('5999-01-01 00:00:00' AS TIMESTAMP)
        """

        appended_rows = f"""
            CREATE TABLE {trg_table_catalog}.{self.wrk_schema}.appended_rows AS
            SELECT                 
                t1.{', t1.'.join(src_columns)}, 
                t1.valid_from_dttm, 
                t1.valid_to_dttm
            FROM
                {trg_table_catalog}.{self.wrk_schema}.current_slice t1
            LEFT JOIN
                {trg_table_catalog}.{self.wrk_schema}.previous_slice t2 
                ON {join_cond}
            WHERE
                t2.valid_from_dttm IS NULL
        """

        deleted_rows = f"""
            CREATE TABLE cdh_ice.cdh_cvm_wrk.deleted_rows AS
            SELECT                 
                t1.{', t1.'.join(src_columns)},
                t1.valid_from_dttm,
                t3.valid_to_dttm
            FROM
                {trg_table_catalog}.{self.wrk_schema}.previous_slice t1
            LEFT JOIN
                {trg_table_catalog}.{self.wrk_schema}.current_slice t2 
                ON {join_cond}
            CROSS JOIN
                (
                    SELECT valid_from_dttm AS valid_to_dttm
                    FROM {trg_table_catalog}.{self.wrk_schema}.current_slice
                    LIMIT 1
                ) t3
            WHERE
                t2.valid_from_dttm IS NULL
        """

        updated_rows = f"""
            CREATE TABLE cdh_ice.cdh_cvm_wrk.updated_rows AS
            SELECT                 
                t1.{', t1.'.join(src_columns)},
                t1.valid_from_dttm, 
                t1.valid_to_dttm
            FROM
                {trg_table_catalog}.{self.wrk_schema}.current_slice t1
            LEFT JOIN
                {trg_table_catalog}.{self.wrk_schema}.previous_slice t2 
                ON {join_cond}
            WHERE
                t1.hashdiff_key != t2.hashdiff_key
        """

        closed_rows = f"""
            CREATE TABLE cdh_ice.cdh_cvm_wrk.closed_rows AS
            SELECT
                t1.{', t1.'.join(src_columns)},
                t1.valid_from_dttm,                
                t2.valid_from_dttm AS valid_to_dttm
            FROM
                {trg_table_catalog}.{self.wrk_schema}.previous_slice t1
            LEFT JOIN
                {trg_table_catalog}.{self.wrk_schema}.current_slice t2 
                ON {join_cond}
            WHERE
                t1.hashdiff_key != t2.hashdiff_key
        """

        load_batch = f"""
            CREATE TABLE {trg_table_catalog}.{self.wrk_schema}.load_batch AS
            SELECT
                {', '.join(src_columns)},   
                valid_from_dttm,
                valid_to_dttm
            FROM 
                {trg_table_catalog}.{self.wrk_schema}.appended_rows
            UNION ALL
            SELECT
                {', '.join(src_columns)},   
                valid_from_dttm,
                valid_to_dttm
            FROM 
                {trg_table_catalog}.{self.wrk_schema}.deleted_rows
            UNION ALL
            SELECT
                {', '.join(src_columns)},   
                valid_from_dttm,
                valid_to_dttm
            FROM 
                {trg_table_catalog}.{self.wrk_schema}.updated_rows
            UNION ALL
            SELECT
                {', '.join(src_columns)},
                valid_from_dttm,
                valid_to_dttm
            FROM 
                {trg_table_catalog}.{self.wrk_schema}.closed_rows
        """

        final_merge = f"""
            MERGE INTO {self.trg_table} AS t1
            USING {trg_table_catalog}.{self.wrk_schema}.load_batch t2
                ON {join_cond}
                AND t1.valid_from_dttm = t2.valid_from_dttm
            WHEN MATCHED AND (    
                {' OR '.join([f"t2.{col} IS DISTINCT FROM t1.{col}" for col in src_columns_wo_key])}
                OR t2.valid_to_dttm IS DISTINCT FROM t1.valid_to_dttm                            
            )
            THEN UPDATE SET    
                {', '.join([f"{col} = t2.{col}" for col in src_columns_wo_key])},
                valid_to_dttm = t2.valid_to_dttm,
                dataflow_dttm = DATE_TRUNC('SECOND', CURRENT_TIMESTAMP)
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(src_columns)},
                valid_from_dttm,
                valid_to_dttm,
                dataflow_dttm
            ) 
            VALUES (	
                t2.{', t2.'.join(src_columns)},                
                t2.valid_from_dttm,
                t2.valid_to_dttm,
                DATE_TRUNC('SECOND', CURRENT_TIMESTAMP)
            )
        """
        # Combine all queries for execution
        final_query = f"""
            {query_drop_tmp};
            {current_slice};
            {previous_slice};
            {appended_rows};
            {deleted_rows};
            {updated_rows};
            {closed_rows};
            {load_batch};        
            {final_merge};
        """
        return final_query

    def execute(self, context):
        query = self.generate_sql()
        query_list = [item.strip() for item in query.split(';') if item.strip()]
        for query in query_list:
            print(query)
            self.run_query(query)
