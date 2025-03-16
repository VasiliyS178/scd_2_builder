import sys
import logging
from subprocess import Popen, PIPE
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import BooleanType
from settings import settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s %(name)s %(message)s", datefmt=None)
logger = logging.getLogger(__name__)

PATH_TO_HIVE_WAREHOUSE = "/hive/warehouse"


def get_data_slice_on_target_date(
    spark: SparkSession, scd_table_name: str, primary_key: list, target_dt: str
) -> DataFrame:
    scd_df = spark.table(scd_table_name)
    data_slice = scd_df \
        .where(F.col("valid_from_dt") <= F.to_date(F.lit(target_dt))) \
        .select(
            '*',
            F.row_number().over(
                Window.partitionBy(*primary_key).orderBy(F.col('valid_from_dt').desc())
            ).alias('row_number')
        ) \
        .where(F.col('row_number') == 1) \
        .where(F.col("valid_to_dt") >= F.to_date(F.lit(target_dt))) \
        .drop('row_number')
    return data_slice


def add_composite_key(input_df: DataFrame, primary_key: list) -> DataFrame:
    composite_df = input_df
    for col_nm in primary_key:
        composite_df = composite_df.withColumn(f'{col_nm}_copy', F.col(col_nm))

    composite_df = composite_df \
        .fillna(0, primary_key) \
        .fillna('*', primary_key) \
        .fillna(0.0, primary_key) \
        .fillna(False, primary_key) \
        .select(
            '*',
            F.concat_ws('|', *primary_key).alias('composite_key')
        )
    composite_df = composite_df.drop(*primary_key)

    for col_nm in primary_key:
        composite_df = composite_df.withColumnRenamed(f'{col_nm}_copy', col_nm)

    return composite_df


def add_tech_columns(input_df: DataFrame, valid_from_dt: str, valid_to_dt: str, hash_columns: list) -> DataFrame:
    hashed_df = input_df \
        .fillna(0) \
        .fillna('*') \
        .fillna(0.0) \
        .fillna(False) \
        .select(
            'composite_key',
            F.concat_ws('|', *hash_columns).alias('concat_ws_col')
        ) \
        .select(
            'composite_key',
            F.md5(F.col('concat_ws_col')).alias('tech_hash_id'),
            F.to_date(F.lit(valid_from_dt)).alias('valid_from_dt'),
            F.to_date(F.lit(valid_to_dt)).alias('valid_to_dt')
        )
    return hashed_df


def create_init_table(
    spark: SparkSession,
    source_db: str,
    source_table: str,
    partition_column: str,
    start_date: str,
    primary_key: list,
    valid_to_dt: str,
    init_num_partitions: int,
    target_db: str,
    target_table: str,
    need_to_mask_pd: bool,
    pd_columns: list,
    **kwargs
) -> list:
    init_df = spark \
        .read \
        .orc(f"{PATH_TO_HIVE_WAREHOUSE}/{source_db}.db/{source_table}/{partition_column}={start_date}") \
        .dropDuplicates(primary_key)

    target_columns = init_df.columns

    prepared_df = mask_personal_data(init_df, pd_columns) if need_to_mask_pd else init_df

    prepared_df \
        .withColumn('valid_from_dt', F.to_date(F.lit(start_date))) \
        .withColumn('valid_to_dt', F.to_date(F.lit(valid_to_dt))) \
        .withColumn('dataflow_dt', F.to_date(F.lit(start_date))) \
        .repartition(init_num_partitions) \
        .write \
        .mode("overwrite") \
        .format("orc") \
        .option("compression", "zlib") \
        .partitionBy("dataflow_dt") \
        .saveAsTable(f"{target_db}.{target_table}")

    return target_columns


def append_delta_to_scd_table(
    spark: SparkSession,
    source_db: str,
    source_table: str,
    partition_column: str,
    previous_date: str,
    process_date: str,
    target_columns: list,
    tech_columns: list,
    primary_key: list,
    valid_to_dt: str,
    num_partitions: int,
    target_db: str,
    target_table: str,
    need_to_mask_pd: bool,
    pd_columns: list,
    **kwargs
) -> None:
    """Find new, deleted and updated rows to save them as delta to SCD-2 table. It uses previous slice and slice on the
    process date (current) to find changes.
    """
    previous_df = spark \
        .read \
        .orc(f"{PATH_TO_HIVE_WAREHOUSE}/{source_db}.db/{source_table}/{partition_column}={previous_date}") \
        .select(target_columns) \
        .dropDuplicates(primary_key)

    previous_df = add_composite_key(previous_df, primary_key)

    previous_masked_df = mask_personal_data(previous_df, pd_columns) if need_to_mask_pd else previous_df

    previous_hashed_df = add_tech_columns(
        input_df=previous_masked_df,
        valid_from_dt=process_date,
        valid_to_dt=previous_date,
        hash_columns=target_columns
    )

    previous_pre_df = previous_masked_df.join(previous_hashed_df, on='composite_key', how='inner')

    current_df = spark \
        .read \
        .orc(f"{PATH_TO_HIVE_WAREHOUSE}/{source_db}.db/{source_table}/{partition_column}={process_date}") \
        .select(target_columns) \
        .dropDuplicates(primary_key)

    current_df = add_composite_key(current_df, primary_key)

    current_masked_df = mask_personal_data(current_df, pd_columns) if need_to_mask_pd else current_df

    current_hashed_df = add_tech_columns(
        input_df=current_masked_df,
        valid_from_dt=process_date,
        valid_to_dt=valid_to_dt,
        hash_columns=target_columns
    )

    current_pre_df = current_masked_df.join(current_hashed_df, on='composite_key', how='inner')

    new_rows = current_pre_df \
        .join(previous_pre_df.select('composite_key'), on='composite_key', how='leftanti')

    deleted_rows = previous_pre_df \
        .join(current_pre_df.select('composite_key'), on='composite_key', how='leftanti')

    updated_rows = current_pre_df \
        .join(
            previous_pre_df.select('composite_key', F.col('tech_hash_id').alias('prev_tech_hash_id')),
            on='composite_key',
            how='inner'
        ) \
        .filter('tech_hash_id != prev_tech_hash_id') \
        .drop('prev_tech_hash_id')

    empty_df = spark.createDataFrame([], schema=previous_pre_df.schema)

    union_df = empty_df.unionByName(new_rows)
    union_df = union_df.unionByName(deleted_rows)
    union_df = union_df.unionByName(updated_rows)

    union_df \
        .withColumn('dataflow_dt', F.to_date(F.lit(process_date))) \
        .select(target_columns + tech_columns) \
        .repartition(num_partitions) \
        .write \
        .format('orc') \
        .option('compression', 'zlib') \
        .insertInto(f"{target_db}.{target_table}", overwrite=True)


def get_dates_to_process(src_db: str, src_table: str) -> list:
    src_path = f"{PATH_TO_HIVE_WAREHOUSE}/{src_db}.db/{src_table}"
    proc = Popen(f"hdfs dfs -ls {src_path}", shell=True, stdout=PIPE)
    std_out = proc.communicate()[0].decode('utf-8')
    date_list = sorted([x.split("=")[-1] for x in std_out.split("\n") if "=" in x])
    return date_list


def mask_personal_data(df: DataFrame, column_names: list) -> DataFrame:
    """Replace personal data in specific columns with True (if it is not null) and False values
    """
    output = df
    for column_name in column_names:
        output = output.withColumn(
            column_name, F.when(F.col(column_name).isNull(), False).otherwise(F.lit(True)).cast(BooleanType())
        )
    return output


def main():
    target_table = sys.argv[1]
    conf = getattr(settings, target_table.replace(".", "_"))
    conf['ds'] = sys.argv[2]

    spark = SparkSession.builder.appName(target_table).master("yarn").enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.hive.caseSensitiveInferenceMode', 'INFER_ONLY')
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sparkContext.setLogLevel("WARN")

    conf["spark"] = spark
    conf["tech_columns"] = ["valid_from_dt", "valid_to_dt", "dataflow_dt"]

    # try to read SCD-table. If table is not created then create it from the first slice on the start date
    try:
        scd = spark.table(f"{conf['target_db']}.{conf['target_table']}")
        conf['target_columns'] = [column for column in scd.columns if column not in conf['tech_columns']]
        conf['previous_date'] = str(
            scd
            .select("dataflow_dt")
            .distinct()
            .sort("dataflow_dt")
            .collect()[-1]["dataflow_dt"]
        )
    except (AnalysisException, IndexError):
        logger.info("Start to build init SCD-2 table")
        conf['previous_date'] = conf['start_date']
        conf['target_columns'] = create_init_table(**conf)

    dates_to_process = get_dates_to_process(conf["source_db"], conf["source_table"])

    # filter already processed dates
    filtered_dates = []
    for date in dates_to_process:
        if conf['previous_date'] < date <= conf['ds']:
            filtered_dates.append(date)

    logger.info(f"filtered_dates:\n{filtered_dates}")

    for i, process_date in enumerate(filtered_dates):
        logger.info(f"Start to append changes on: {process_date}")
        conf['process_date'] = process_date
        if i > 0:
            conf['previous_date'] = filtered_dates[i-1]

        append_delta_to_scd_table(**conf)


if __name__ == "__main__":
    main()
