import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, SaveMode, functions => f}
import org.apache.spark.sql.types._
import java.sql.Types
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcDialect, JdbcType}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.shell.Truncate

object Test {
def main(args: Array[String]):Unit = {
val spark: SparkSession = SparkSession.builder
.appName("test")
.config("spark.master", "local")
.getOrCreate()

val df = spark
.read.format("kafka")
.option("kafka.bootstrap.servers", " --- hello ---")
.option("subscribe", "--- hello ---")
.option("failOnDataLoss", "false")
.option("startingOffsets", "earliest")
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "SCRAM-SHA-512")
.option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"--- hello ---\" password=\--- hello ---\";")
.option("kafka.ssl.ca.location", "--- hello ---")
.load()

df.persist()

if (df.head(1).isEmpty) {println("DATAFRAME FROM KAFKA IS EMPTY")} else {println(s"DATAFRAME FROM KAFKA GETED, STRING COUNT: ${df.count()}")}

val schema_currency = StructType(Seq(
    StructField("object_type", StringType),
    StructField("payload", StructType(Seq(
        StructField("date_update", TimestampType),
        StructField("currency_code", IntegerType),
        StructField("currency_code_with", IntegerType),
        StructField("currency_with_div", DecimalType(5,2))))
    )))

val schema_transaction = StructType(Seq(
    StructField("object_type", StringType, true),
    StructField("payload", StructType(Seq(
        StructField("operation_id", StringType, true),
        StructField("account_number_from", IntegerType, true),
        StructField("account_number_to", IntegerType, true),
        StructField("currency_code", IntegerType, true),  
        StructField("country", StringType, true),
        StructField("status", StringType, true),
        StructField("transaction_type", StringType, true),
        StructField("amount", DecimalType(15,2), true),
        StructField("transaction_dt", TimestampType, true)))
    )))

val df_currency = df
.withColumn("value", f.col("value")
.cast(StringType))
.select(f.col("value"))
.withColumn("value", f.from_json(f.col("value"), schema_currency))
.select(f.col("value.*"))
.where("object_type == \"CURRENCY\"")
.select(f.col("payload.*"))
.na.drop()
.distinct()

if (df_currency.head(1).isEmpty) {println("DATAFRAME CURRENCY FROM KAFKA IS EMPTY")} 
else {println(s"DATAFRAME CURRENCY FROM KAFKA. STRING COUNT: ${df_currency.count()}")
      df_currency.limit(1).show()}        

val df_transaction = df
.withColumn("value", f.col("value")
.cast(StringType))
.select(f.col("value"))
.withColumn("value", f.from_json(f.col("value"), schema_transaction))
.select(f.col("value.*"))
.where("object_type == \"TRANSACTION\"")
.select(f.col("payload.*"))
.na.drop()
.distinct()

if (df_transaction.head(1).isEmpty) {println("DATAFRAME TRANSACTION FROM KAFKA IS EMPTY")} 
else {println(s"DATAFRAME TRANSACTION FROM KAFKA. STRING COUNT: ${df_transaction.count()}")
      df_transaction.limit(1).show()}

val url = "jdbc:vertica://--- hello ---?user=--- hello ---&password=--- hello ---"

df_currency.write
.format("jdbc")
.mode("overwrite")
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", "HELLYDEYAYANDEXRU__STAGING.currencies")
.save()

val df_read_vertica_currency = spark.read.format("jdbc")
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", "HELLYDEYAYANDEXRU__STAGING.currencies")
.load()

if (df_read_vertica_currency.head(1).isEmpty) {println("STAGING CURRENCY IS EMPTY, GO HARD")} 
else {println(s"STAGING CURRENCY STRING COUNT: ${df_read_vertica_currency.count()}")
      df_read_vertica_currency.limit(1).show()}


val VerticaDialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:vertica")
    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] ={
        typeName match {
            case "Varchar" => Some(StringType)
            case "Numeric" => Some(DecimalType(15,2))
            case "Date" => Some(DateType)
            case "Integer" => Some(IntegerType)
            case "Timestamp" => Some(TimestampType)
        }
    }
    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Option(JdbcType("VARCHAR", Types.VARCHAR))
        case IntegerType => Option(JdbcType("INTEGER", Types.INTEGER))
        case TimestampType => Option(JdbcType("TIMESTAMP", Types.TIMESTAMP))
        case _ => None
    }
}

JdbcDialects.registerDialect(VerticaDialect)

df_transaction.write
.format("jdbc")
.mode(SaveMode.Overwrite)
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", "--- hello ---")
.save()

val df_read_vertica_transactions = spark.read.format("jdbc")
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", "--- hello ---")
.load()

if (df_read_vertica_transactions.head(1).isEmpty) {println("STAGING TRANSACTION IS EMPTY, GO HARD")} 
else {println(s"STAGING TRANSACTION STRING COUNT: ${df_read_vertica_transactions.count()}")
      df_read_vertica_transactions.limit(1).show()}


/* --------------FULL--------------*/
val df_normalize_transaction = df_read_vertica_transactions
.where("account_number_from > 0 and transaction_type like ('%_incoming') and status = 'done'")

val df_normalize_currencies = df_read_vertica_currency
.where("currency_code_with = 420")

val df_join = df_normalize_transaction.as("t")
.join(df_normalize_currencies.as("c"), (f.col("t.currency_code") === f.col("c.currency_code") and f.to_date(f.col("t.transaction_dt")) === f.col("c.date_update")), "left")
.selectExpr("operation_id", "account_number_from", "account_number_to", "t.currency_code", "coalesce(currency_with_div, 1) currency_with_div", "amount", "to_date(transaction_dt) date_update", "transaction_dt")

df_join.createOrReplaceTempView("TMP")

val df_result = spark.sql("select first(date_update) date_update, first(currency_code) currency_code, sum(amount*currency_with_div) amount_total," +
  "count(amount) cnt_transactions, " +
  "(sum(amount * currency_with_div) / count(distinct operation_id)) avg_transactions_per_account, " +
  "count(distinct account_number_from) as cnt_accounts_make_transactions " +
  "from TMP group by (date_update, currency_code)")

if (df_result.head(1).isEmpty) {println("TEMP VIEW IS EMPTY, GO HARD")} 
else {println(s"TEMP VIEW STRING COUNT: ${df_result.count()}")
      df_result.limit(1).show()}     

df_result.write
.format("jdbc")
.mode(SaveMode.Overwrite)
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", "--- hello ---")
.option("transaction.retries", 3)
.option("transaction.retry.timeout", 10)
.save()

val df_read_vertica_global_metrics = spark.read.format("jdbc")
.option("driver", "com.vertica.jdbc.Driver")
.option("url", url)
.option("dbtable", --- hello ---")
.load()

if (df_read_vertica_global_metrics.head(1).isEmpty) {println("DWH GLOBAL METRICS IS EMPTY, GO HARD")} 
else {println(s"DWH GLOBAL METRICS STRING COUNT: ${df_read_vertica_global_metrics.count()}")
      df_read_vertica_global_metrics.show(100, false) }   

df.unpersist()
