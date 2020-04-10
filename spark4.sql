import org.apache.spark.sql.hive._

//HiveContext supports window functions therefore it is used instead of SqlContext
//if hive context is initialized once the following code could be commented out
val sqlContext = new HiveContext(sc)
val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // first line will be used as header
    .option("inferSchema", "true") // infer data types
    .option("delimiter", ";") //delimeter ;
    .load("file:/C:/Users/Tuna.Tore/Desktop/zeppelin-0.6.2-bin-all/zeppelin-0.6.2-bin-all/bin/kaggle_geo.csv")//.cache() //the location of the CSV file.

df.registerTempTable("kaggle_geo") //temp table
println("total record count " + df.count)