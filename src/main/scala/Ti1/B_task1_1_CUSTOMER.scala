package Ti1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object B_task1_1_CUSTOMER {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("B_task1_1_CUSTOMER")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    sparkSession.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
//      .option("")
  }
}
