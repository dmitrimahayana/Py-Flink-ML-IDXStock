from pyflink.table import *
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table.expressions import col

# Prepare your JAR file URIs
jars_path = "D:/00%20Project/00%20My%20Project/Jars/Kafka%201.18/"
jar_files = [
    "file:///" + jars_path + "avro-1.11.3.jar",
    "file:///" + jars_path + "bson-4.7.2.jar",
    "file:///" + jars_path + "flink-avro-1.18.0.jar",
    "file:///" + jars_path + "flink-avro-confluent-registry-1.18.0.jar",
    "file:///" + jars_path + "flink-connector-mongodb-1.0.2-1.17.jar",
    "file:///" + jars_path + "flink-sql-connector-kafka-3.0.2-1.18.jar",
    "file:///" + jars_path + "guava-30.1.1-jre.jar",
    "file:///" + jars_path + "jackson-annotations-2.12.5.jar",
    "file:///" + jars_path + "jackson-core-2.12.5.jar",
    "file:///" + jars_path + "jackson-databind-2.12.5.jar",
    "file:///" + jars_path + "kafka-clients-3.6.0.jar",
    "file:///" + jars_path + "kafka-schema-registry-client-7.5.0.jar",
    "file:///" + jars_path + "mongodb-driver-sync-4.7.2.jar",
    "file:///" + jars_path + "mongodb-driver-core-4.7.2.jar",
]
jar_files_str = ";".join(jar_files)

# Set the configuration
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
table_env = StreamTableEnvironment.create(env)
table_env.get_config().set("pipeline.jars", jar_files_str)
table_env.get_config().set("parallelism.default", "4")

# Kafka Config
topic1 = "KSQLTABLEGROUPSTOCK"  # KSQLDB Table
group = "flink-group-idx-stock-mongodb-consumer"
kafka_bootstrap_server = "localhost:19092,localhost:19093,localhost:19094"
kafka_schema_server = "http://localhost:8282"
offset = 'earliest-offset'  # Use earliest-offset OR latest-offset

# KAFKA SQL TABLE MUST USE UPPERCASE COLUMN NAME
table_env.execute_sql("CREATE TABLE flink_ksql_groupstock (" +
                      "  `EVENT_TIME` TIMESTAMP(3) METADATA FROM 'timestamp', " +
                      "  `STOCKID` STRING, " +
                      "  `TICKER` STRING, " +
                      "  `DATE` STRING, " +
                      "  `OPEN` DOUBLE, " +
                      "  `HIGH` DOUBLE, " +
                      "  `LOW` DOUBLE, " +
                      "  `CLOSE` DOUBLE, " +
                      "  `VOLUME` BIGINT " +
                      ") WITH (" +
                      "  'connector' = 'kafka', " +
                      "  'topic' = '" + topic1 + "', " +
                      "  'properties.bootstrap.servers' = '" + kafka_bootstrap_server + "', " +
                      "  'properties.group.id' = '" + group + "', " +
                      "  'scan.startup.mode' = '" + offset + "', " +
                      "  'value.format' = 'avro-confluent', " +
                      "  'value.avro-confluent.url' = '" + kafka_schema_server + "' " +
                      ");")

# Define a query
table_output1 = table_env.sql_query("SELECT * FROM flink_ksql_groupstock WHERE `DATE` LIKE '%2023-06-2%'") \
    .select(col("STOCKID").alias("stockid"),
            col("TICKER").alias("ticker"),
            col("DATE").alias("date"),
            col("OPEN").alias("open"),
            col("HIGH").alias("high"),
            col("LOW").alias("low"),
            col("CLOSE").alias("close"),
            col("VOLUME").alias("volume"))

# Create dummy view
table_env.create_temporary_view("filtered_ksql_groupstock", table_output1)

# Table API Mongodb
table_env.execute_sql("CREATE TABLE flink_stock_update (" +
                      "  _id STRING," +
                      "  `ticker` STRING," +
                      "  `date` STRING," +
                      "  `open` DOUBLE," +
                      "  `high` DOUBLE," +
                      "  `low` DOUBLE," +
                      "  `close` DOUBLE," +
                      "  `volume` BIGINT," +
                      "  PRIMARY KEY (_id) NOT ENFORCED" +
                      "  ) WITH (" +
                      "  'connector' = 'mongodb'," +
                      "  'uri' = 'mongodb://localhost:27017'," +
                      "  'database' = 'kafka'," +
                      "  'collection' = 'flink-stock-stream'" +
                      ");")

# Query Table API
table_env.execute_sql(f"""
    INSERT INTO flink_stock_update
    SELECT `stockid`, `ticker`, `date`, `open`, `high`, `low`, `close`, `volume` FROM filtered_ksql_groupstock;
""")

table_output2 = table_env.sql_query("SELECT _id, `ticker`, `date`, `close` FROM flink_stock_update")
table_output2.execute().print()
