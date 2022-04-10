package qiyexiaofeifuwu

import org.apache.spark.{SparkConf, SparkContext}

object spark05_Spark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark05_Spark")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("src/main/resources/accommodation_output1/part-00000")
    lines.cache()
    lines.map(
      line=>{
        val city = line.split(",")(4)
        val changsuo = line.split(",")(0)
        val room = line.split(",")(8)
        (city,changsuo,room)
      }
    )

  }
}
