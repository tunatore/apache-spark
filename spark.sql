val geo = sc.textFile("file:/C:/Users/Tuna.Tore/Desktop/zeppelin-0.6.2-bin-all/zeppelin-0.6.2-bin-all/bin/kaggle_geo.csv")

case class Kaggle_Geo(user: String, sku: String, category: String, query: String, click_time: String, query_time: String, city: String, country: String, location: String, time_spent: String)

val kaggle_geo = geo.map(line => line.split(";"))
                        .filter(line => line(0) != "user")
                        .map(line => Kaggle_Geo(line(0),line(1),line(2), line(3).toLowerCase, line(4), line(5).take(10), line(6), line(7), line(8), line(9))
)

val df = kaggle_geo.toDF();

println("Total record count: " + df.count);