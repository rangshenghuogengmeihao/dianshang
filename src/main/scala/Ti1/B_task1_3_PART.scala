package Ti1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object B_task1_3_PART {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("B_task1_3_PART")
    val sparkSession = SparkSession.builder()
      .config("hive.metastore.uris", "thrift://192.168.23.69:9083")
      .config("spark.sql.warehouse.dir",
        "hdfs://192.168.23.69:9000/user/hive/warehouse")
      .config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/shtd_store?useSSL=false")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","PART")
      .load()
      .createTempView("mysql_PART")

    sparkSession.sql("insert overwrite table ods.park partition (etldate='2022-4-9') select * from mysql_PART")

    sparkSession.close()
  }
}
