package dianshang

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object testC_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("testC_2")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame = sparkSession.read.json("src/main/resources/ORDERS/part-00000-24f268cf-8ece-410b-a7cb-6e687ea9c30f.json")
    dataFrame.select()
  }
}
