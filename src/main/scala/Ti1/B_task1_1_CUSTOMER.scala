package Ti1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object B_task1_1_CUSTOMER {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("B_task1_1_CUSTOMER")
    val sparkSession = SparkSession.builder()
      .config("hive.metastore.uris", "thrift://192.168.23.69:9083")
      .config("spark.sql.warehouse.dir",
        "hdfs://192.168.23.69:9000/user/hive/warehouse")
      .config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.23.69:3306/shtd_store?useSSL=false")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","CUSTOMER")
      .load()
      .createTempView("mysql_CUSTOMER")

//    val structType = StructType(List(
//      StructField("CUSTKEY", StringType, nullable = true),
//      StructField("NAME", StringType, nullable = true),
//      StructField("ADDRESS", StringType, nullable = true),
//      StructField("NATIONKEY", StringType, nullable = true),
//      StructField("PHONE", StringType, nullable = true),
//      StructField("ACCTBAL", StringType, nullable = true),
//      StructField("MKTSEGMENT", StringType, nullable = true),
//      StructField("COMMENT", StringType, nullable = true)
//    ))

    sparkSession.sql(
      s"""
         |insert overwrite table ods.customer partition (etldate='2022-4-9')
         |select * from mysql_CUSTOMER
  """.stripMargin)

    sparkSession.close()
  }
}
