from pyflink.table import *
from pyflink.table.expressions import col
from pyflink.ml.feature.onehotencoder import OneHotEncoder
from pyflink.ml.feature.standardscaler import StandardScaler
from pyflink.ml.feature.univariatefeatureselector import UnivariateFeatureSelector
from pyflink.ml.feature.vectorassembler import VectorAssembler
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.ml.regression.linearregression import LinearRegression
from pyflink.ml.feature.stringindexer import StringIndexer

# Prepare your JAR file URIs
jars_path = "D:/00%20Project/00%20My%20Project/Jars/Kafka%201.17/"
jar_files = [
    "file:///" + jars_path + "avro-1.11.0.jar",
    "file:///" + jars_path + "flink-avro-1.17.1.jar",
    "file:///" + jars_path + "flink-avro-confluent-registry-1.17.1.jar",
    "file:///" + jars_path + "flink-sql-connector-kafka-1.17.1.jar",
    "file:///" + jars_path + "flink-sql-connector-mongodb-1.0.1-1.17.jar",
    "file:///" + jars_path + "guava-30.1.1-jre.jar",
    "file:///" + jars_path + "jackson-annotations-2.12.5.jar",
    "file:///" + jars_path + "jackson-core-2.12.5.jar",
    "file:///" + jars_path + "jackson-databind-2.12.5.jar",
    "file:///" + jars_path + "kafka-clients-3.2.3.jar",
    "file:///" + jars_path + "kafka-schema-registry-client-7.4.0.jar",
    # "file:///" + jars_path + "bson-4.7.2.jar",
    # "file:///" + jars_path + "flink-connector-files-1.17.1.jar",
    # "file:///" + jars_path + "flink-connector-kafka-1.17.1.jar",
    # "file:///" + jars_path + "flink-connector-mongodb-1.0.1-1.17.jar",
    # "file:///" + jars_path + "flink-ml-uber-1.17-2.3.0.jar",
    # "file:///" + jars_path + "flink-table-runtime-1.17.1.jar",
    # "file:///" + jars_path + "mongodb-driver-core-4.7.2.jar",
    # "file:///" + jars_path + "mongodb-driver-sync-4.7.2.jar",
    # "file:///" + jars_path + "statefun-flink-core-3.2.0.jar",
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
group = "flink-group-idx-stock-consumer"
kafka_bootstrap_server = "localhost:19092,localhost:19093,localhost:19094"
kafka_schema_server = "http://localhost:8282"
offset = 'earliest-offset'  # Use earliest-offset OR latest-offset

# Table API Kafka SQL TABLE MUST USE UPPERCASE COLUMN NAME
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
                      ")")

# Define a query
# table_input = table_env.sql_query("SELECT * FROM flink_ksql_groupstock WHERE `DATE` LIKE '%2023-06%'") \
#     .select(col("TICKER").alias("ticker"),
#             col("DATE").alias("date"),
#             col("OPEN").alias("open"),
#             col("HIGH").alias("high"),
#             col("LOW").alias("low"),
#             col("CLOSE").alias("close"),
#             col("VOLUME").alias("volume"))

# Table API mongodb
table_env.execute_sql("CREATE TABLE flink_mongodb_stock (" +
                      "  `id` STRING, " +
                      "  `ticker` STRING, " +
                      "  `date` STRING, " +
                      "  `open` DOUBLE, " +
                      "  `high` DOUBLE, " +
                      "  `low` DOUBLE, " +
                      "  `close` DOUBLE " +
                      ") WITH (" +
                      "   'connector' = 'mongodb'," +
                      "   'uri' = 'mongodb://localhost:27017'," +
                      "   'database' = 'kafka'," +
                      "   'collection' = 'ksql-stock-stream'" +
                      ");")

# Define a query
table_input = table_env.sql_query("SELECT * FROM flink_mongodb_stock WHERE `date` LIKE '%2023-06%'")

# Creates a StringIndexer object and initializes its parameters.
string_indexer = StringIndexer() \
    .set_string_order_type('alphabetAsc') \
    .set_input_cols("ticker", "date") \
    .set_output_cols("tickerIndex", "dateIndex")

# Trains the StringIndexer Model.
indexer_model = string_indexer.fit(table_input)

# Uses the StringIndexer Model for predictions.
indexer_table = indexer_model.transform(table_input)[0]
print(indexer_table.print_schema())

# Creates a OneHotEncoder object and initializes its parameters.
one_hot_encoder = OneHotEncoder() \
    .set_input_cols("tickerIndex", "dateIndex") \
    .set_output_cols("tickerOneHot", "dateOneHot")

# Trains the OneHotEncoder Model.
one_hot_model = one_hot_encoder.fit(indexer_table)

# Uses the OneHotEncoder Model for predictions.
one_hot_table = one_hot_model.transform(indexer_table)[0]
print(one_hot_table.print_schema())

# Get Ticker and Date Vector Size
tickerSize = 0
dateSize = 0
field_names = one_hot_table.get_schema().get_field_names()
for row in table_env.to_data_stream(one_hot_table).execute_and_collect():
    tickerVec = row[field_names.index("tickerOneHot")]  # Assuming this is a SparseVector
    dateVec = row[field_names.index("dateOneHot")]  # Assuming this is a SparseVector
    tickerSize = len(tickerVec)
    dateSize = len(dateVec)
    print(f"TickerOneHot Vec Size: {tickerSize}\tDateOneHot Vec Size: {dateSize}")
    break

# Creates a VectorAssembler object and initializes its parameters.
vector_assembler = VectorAssembler() \
    .set_input_cols("tickerOneHot", "dateOneHot", "open", "high", "low") \
    .set_input_sizes(tickerSize, dateSize, 1, 1, 1) \
    .set_output_col("vectorAssembly")

# Uses the VectorAssembler object for feature transformations.
vector_table = vector_assembler.transform(one_hot_table)[0]
print(vector_table.print_schema())

# Creates a StandardScaler object and initializes its parameters.
standard_scale = StandardScaler() \
    .set_input_col("vectorAssembly") \
    .set_output_col("scaledFeatures")

# Trains the StandardScaler Model.
scale_model = standard_scale.fit(vector_table)

# Uses the StandardScaler Model for predictions.
scale_table = scale_model.transform(vector_table)[0]
print(scale_table.print_schema())

# Creates a UnivariateFeatureSelector object and initializes its parameters.
univariate_feature_selector = UnivariateFeatureSelector() \
    .set_features_col("scaledFeatures") \
    .set_label_col("close") \
    .set_output_col("newFeatures") \
    .set_feature_type("continuous") \
    .set_label_type("continuous") \
    .set_selection_mode("numTopFeatures") \
    .set_selection_threshold(1)

# Trains the UnivariateFeatureSelector model.
feature_selector_model = univariate_feature_selector.fit(scale_table)

# Uses the UnivariateFeatureSelector model for predictions.
feature_selector_table = feature_selector_model.transform(scale_table)[0]
print(feature_selector_table.print_schema())

# Creates a LinearRegression object and initializes its parameters.
lr = LinearRegression() \
    .set_features_col("newFeatures") \
    .set_label_col("close") \
    .set_reg(0.3) \
    .set_elastic_net(0.8)

# Trains the LinearRegression Model.
lr_model = lr.fit(feature_selector_table)

# Uses the LinearRegression Model for predictions.
predict_table = lr_model.transform(feature_selector_table)[0]

# extract and display the results
field_names = predict_table.get_schema().get_field_names()
for result in table_env.to_data_stream(predict_table).execute_and_collect():
    features = result[field_names.index(lr.get_features_col())]
    expected_result = result[field_names.index(lr.get_label_col())]
    prediction_result = result[field_names.index(lr.get_prediction_col())]
    print('Features: ' + str(features) + ' \tExpected Result: ' + str(expected_result)
          + ' \tPrediction Result: ' + str(prediction_result))
