import os
from pyspark.sql.session import SparkSession

os.environ["SPARKEXPECTATIONS_ENV"] = "local"

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

RULES_TABLE_SCHEMA = """ ( product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    tag STRING,
    description STRING,
    enable_for_source_dq_validation BOOLEAN, 
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN,
    enable_error_drop_alert BOOLEAN,
    error_drop_threshold double ,
    query_dq_delimiter STRING,
    enable_querydq_custom_output BOOLEAN
    )
"""#changes by sudeep


RULES_DATA = """

     ("your_product", "dq_spark_{env}.customer_order", "row_dq", "null_check_row", "row_id", "row_id is not null", "ignore", "accuracy", "Null check row_id", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "null_check_ord", "order_date", "order_date is not null", "ignore", "accuracy", "Null check order_date", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "null_check", "customer_id", "customer_id is not null", "ignore", "accuracy", "Null check customer_id", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "junk_check", "customer_id", "customer_id not in  ('[~@^[()] | [[:cntrl:]]')", "ignore", "accuracy", "Junk check", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "date_formate_check", "order_date", "to_date(order_date, 'yyyy/MM/dd') is not null", "ignore", "accuracy", "date format check", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "date_lenght_check", "customer_id", "length(customer_id) between 0 and 8", "ignore", "accuracy", "date length check", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "data_check_cust","customer_id", "(select count(customer_id) from order_source where customer_id not in (select customer_id from order_target))=0", "ignore", "validity", "customer id check", true, false, true, false, 0,null, false)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "data_check_ord","order_id", "(select count(*) from (select a.order_id src, b.order_id trg from order_source a left join order_target b on regexp_replace(trim(a.order_id), '^[0]*', '') = regexp_replace(trim(b.order_id), '^[0]*', '')) where trg is null)=0", "ignore", "validity", "order id check ", true, false, true, false, 0,null, false)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "data_check_recon","*", "(select count(*) from (select a.sales src, b.sales trg from order_source a left join order_target b on regexp_replace(trim(a.customer_id), '^[0]*', '') = regexp_replace(trim(b.customer_id), '^[0]*', '') and regexp_replace(trim(a.sales), '^[0]*', '') = regexp_replace(trim(b.sales), '^[0]*', '')) where trg is null)=0", "ignore", "validity", "Data reconciliation", true, false, true, false, 70.5,null, false)

    """

# RULES_DATA = """
#
#("your_product", "dq_spark_{env}.customer_order", "row_dq", "duplicate_check", "customer_id", "count(*) over(partition by customer_id order by 1)=1", "ignore", "uniqueness", "Duplicate check customer_id",true, false, true, false, 11, null, false)
#
# """
#
# RULES_DATA = """
#
# ("your_product", "dq_spark_{env}.customer_order", "query_dq", "Integrity_check", "sales", "select a.customer_id src_cust, b.customer_id m_cust from order_source a left join order_target b on regexp_replace(trim(a.customer_id), '^[0]*', '') = regexp_replace(trim(b.customer_id), '^[0]*', '') where m_cust is null", "ignore", "Validity", "Data recon", true, false, true, false,50,null, false)
#
# """

# RULES_DATA="""
# ("your_product", "dq_spark_{env}.customer_order", "query_dq", "null_check", "row_id", "(select count(*) from order_source where row_id is null)=0", "ignore", "Accuracy", "Null check", true, false, true, false,50,null, false)
# """
# RULES_DATA = """
#
# ("your_product", "dq_spark_{env}.customer_order", "query_dq", "duplicate_check", "customer_id", "(select sum(c) from (select customer_id, count(*) c from order_source group by 1 having count(*)>1))=0", "ignore", "uniqueness", "Duplicate check", true, false, true, false,0,null, false)
#
# """
# RULES_DATA = """
# ("your_product", "dq_spark_{env}.customer_order", "query_dq", "Data Integrity", "*", "(select count(*) from (select t1.customer_id as src, t2.customer_id as trg from order_source t1 left join order_target t2 on regexp_replace(trim(t1.customer_id), '^[0]*', '') = regexp_replace(trim(t2.customer_id), '^[0]*', '') where t2.customer_id is null))=0", "ignore", "Validity", "data check", true, false, true, false,0,null, false)
# """


# RULES_DATA = """
# ("your_product", "dq_spark_{env}.customer_order", "query_dq", "Integrity_check", "row_id", "(select count(*) from order_source where row_id is null)=0", "ignore", "Validity", "Data recon", true, false, true, false,50,null, false)
# """


def set_up_kafka() -> None:
    print("create or run if exist docker container")
    os.system(f"sh {CURRENT_DIR}/docker_scripts/docker_kafka_start_script.sh")


def add_kafka_jars(builder: SparkSession.builder) -> SparkSession.builder:
    return builder.config(  # below jars are used only in the local env, not coupled with databricks or EMR
        "spark.jars",
        f"{CURRENT_DIR}/../../jars/spark-sql-kafka-0-10_2.12-3.0.0.jar,"
        f"{CURRENT_DIR}/../../jars/kafka-clients-3.0.0.jar,"
        f"{CURRENT_DIR}/../../jars/commons-pool2-2.8.0.jar,"
        f"{CURRENT_DIR}/../../jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar",
    )


def set_up_iceberg() -> SparkSession:
    set_up_kafka()
    spark = add_kafka_jars(
        SparkSession.builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/hive/warehouse")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/tmp/hive/warehouse")
    ).getOrCreate()

    os.system("rm -rf /tmp/hive/warehouse/dq_spark_local")

    spark.sql("create database if not exists spark_catalog.dq_spark_local")
    spark.sql(" use spark_catalog.dq_spark_local")

    spark.sql("drop table if exists dq_spark_local.dq_stats")

    spark.sql("drop table if exists dq_spark_local.dq_rules")

    spark.sql(
        f" CREATE TABLE dq_spark_local.dq_rules {RULES_TABLE_SCHEMA} USING ICEBERG"
    )
    spark.sql(f" INSERT INTO dq_spark_local.dq_rules  values {RULES_DATA} ")

    spark.sql("select * from dq_spark_local.dq_rules").show(truncate=False)
    return spark


def set_up_bigquery(materialization_dataset: str) -> SparkSession:
    set_up_kafka()
    spark = add_kafka_jars(
        SparkSession.builder.config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0",
        )
    ).getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", materialization_dataset)

    # Add dependencies like gcs-connector-hadoop3-2.2.6-SNAPSHOT-shaded.jar, spark-avro_2.12-3.4.1.jar if you wanted to use indirect method for reading/writing
    return spark


def set_up_delta() -> SparkSession:
    set_up_kafka()

    builder = add_kafka_jars(
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "/tmp/hive/warehouse")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .config("spark.jars.ivy", "/tmp/ivy2")
    )
    spark = builder.getOrCreate()

    os.system("rm -rf /tmp/hive/warehouse/dq_spark_dev.db")

    spark.sql("create database if not exists dq_spark_dev")
    spark.sql("use dq_spark_dev")

    spark.sql("drop table if exists dq_stats")

    spark.sql("drop table if exists dq_rules")

    spark.sql(f" CREATE TABLE dq_rules {RULES_TABLE_SCHEMA} USING DELTA")

    spark.sql(f" INSERT INTO dq_rules  values {RULES_DATA}")

    spark.sql("select * from dq_rules").show(truncate=False)

    return spark
