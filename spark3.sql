import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions.{lit, to_date};

//partition by
val byQuery = Window.partitionBy("query").orderBy(asc("query_time"));

//filter records and group by order by
val result = df.filter(to_date(df("query_time")) >= to_date(lit("2011-09-09")) && to_date(df("query_time")) <= to_date(lit("2011-09-10")))
                    .orderBy("query","query_time")
                        .groupBy(df("query"),df("query_time")).agg(count("query"));

val resultlag = result.select("query","query_time","count(query)").withColumn("LAST_QUERY_COUNT", lag("count(query)", 1).over(byQuery));

val resultpercent = resultlag.groupBy(resultlag("query"), resultlag("query_time")).agg(sum((resultlag("count(query)") - resultlag("LAST_QUERY_COUNT")) / resultlag("LAST_QUERY_COUNT") * 100).alias("sumpercent"));

val percent = resultpercent.groupBy(resultpercent("query")).agg(avg(when(resultpercent("sumpercent").isNull, 0).otherwise(resultpercent("sumpercent"))).alias("percent")).orderBy(asc("percent")).limit(10).show()