package dianshang

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object testB {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("testB")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame = sparkSession.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("url", "jdbc:mysql://192.168.23.69:3306/shtd_store?useSSL=false")
      .option("dbtable", "CUSTOMER")
      .load()
//    dataFrame.show()
    dataFrame.write
      .mode("append")
      .json("/user/hive/warehouse/ods.db/customer/")
  }
}
