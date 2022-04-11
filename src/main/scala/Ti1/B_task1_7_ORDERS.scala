package Ti1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object B_task1_7_ORDERS {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("B_task1_7_ORDERS")
    val sparkSession = SparkSession.builder()
      .config("hive.metastore.uris", "thrift://192.168.23.69:9083")
      .config("spark.sql.warehouse.dir",
        "hdfs://192.168.23.69:9000/user/hive/warehouse")
      .config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.23.69:3306/shtd_store?useSSL=false")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","ORDERS")
      .load()
      .createTempView("mysql_ORDERS")

    sparkSession.sql(
      s"""
         |set hive.exec.dynamic.partition = true
         |set hive.exec.dynamic.partition.mode = nonstrict
         |insert overwrite table ods.orders partition (etldate='2022-4-9')
         |select * from mysql_ORDERS
         |insert into table ods. partition ()
  """.stripMargin)

    sparkSession.close()
  }
}
