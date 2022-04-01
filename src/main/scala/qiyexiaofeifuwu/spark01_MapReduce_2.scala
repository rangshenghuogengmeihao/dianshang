package qiyexiaofeifuwu

import org.apache.spark.{SparkConf, SparkContext}

object spark01_MapReduce_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_MapReduce_2")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("src/main/resources/jd_4706.csv")
    lines.cache()
    val result = lines.filter(
      line => {
        val star = line.split(",")(6)
        val comment = line.split(",")(11)
        val grade = line.split(",")(10)
        if (star != "" && comment != "" && grade != "") {
          true
        } else {
          println(line)
          false
        }
      }
    )
    println(result.count())
    sc.stop()
  }
}
