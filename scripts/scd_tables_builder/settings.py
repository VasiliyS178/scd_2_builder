

class Settings:
    schemа_example_table_example_hist = dict(
        start_date='2024-01-01',
        valid_to_dt='9999-12-31',
        source_db="source_schema",
        source_table="source_table_1",
        partition_column="insert_dt",
        primary_key=["id"],
        init_num_partitions=4,
        num_partitions=1,
        target_db="schemа_example",
        target_table="table_example_hist",
        need_to_mask_pd=True,
        pd_columns=[
            'mobile_phone',
            'email'
        ]
    )
    schemа_example_table_example_2_hist = dict(
        start_date='2025-01-01',
        valid_to_dt='9999-12-31',
        source_db="source_schema",
        source_table="source_table_2",
        partition_column="dataflow_dt",
        primary_key=["column_1", "column_2", "column_2"],
        init_num_partitions=10,
        num_partitions=2,
        target_db="schemа_example",
        target_table="table_example_2_hist",
        need_to_mask_pd=False,
        pd_columns=[]
    )


settings = Settings()
