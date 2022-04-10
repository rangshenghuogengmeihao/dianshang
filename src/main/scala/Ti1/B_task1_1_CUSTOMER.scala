package Ti1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object B_task1_1_CUSTOMER {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("B_task1_1_CUSTOMER")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    val dataFrame = sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.23.69:3306/shtd_store")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","CUSTOMER")
      .load()

    val structType = StructType(List(
      StructField("CUSTKEY", StringType, nullable = true),
      StructField("NAME", StringType, nullable = true),
      StructField("ADDRESS", StringType, nullable = true),
      StructField("NATIONKEY", StringType, nullable = true),
      StructField("PHONE", StringType, nullable = true),
      StructField("ACCTBAL", StringType, nullable = true),
      StructField("MKTSEGMENT", StringType, nullable = true),
      StructField("COMMENT", StringType, nullable = true)
    ))


//    dataFrame.show()
    dataFrame.write
//      .format()

    sparkSession.close()
  }
}
