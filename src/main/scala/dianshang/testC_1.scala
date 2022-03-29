package dianshang

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object testC_1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("testC_1")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame = sparkSession.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("url", "jdbc:mysql://192.168.23.69:3306/shtd_store?useSSL=false")
      .option("dbtable", "ORDERS")
      .load()
    dataFrame.write
      .json("src/main/resources/ORDERS")
//    dataFrame.show()
    import sparkSession.implicits._
    val df = sparkSession.read.json("src/main/resources/ORDERS/part-00000-24f268cf-8ece-410b-a7cb-6e687ea9c30f.json")
//    df.createTempView("ORDERS")
    val newdf = df.withColumn("ORDERDATE", 'ORDERDATE.cast("TimeStamp")).distinct()
    newdf.coalesce(1).write.json("src/main/resources/DWD")
  }
}
