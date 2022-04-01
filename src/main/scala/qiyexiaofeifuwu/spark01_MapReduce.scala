package qiyexiaofeifuwu

import org.apache.spark.{SparkConf, SparkContext}

object spark01_MapReduce {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_MapReduce")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("src/main/resources/jd_4706.csv")
    val lines1 = lines.cache()
    val theMax = lines1.map(
      line => {
        val length = line.split(",").length
        length
      }
    ).max()
    val result = lines1.filter(
      _.split(",").length >= theMax - 3
    )
    println("=== “删除缺失值大于3个的字段的数据条数为"+(lines1.count()-result.count())+"条”===")
//    println(theMax)

//    lines.collect().foreach(println)

    sc.stop()
  }
}
