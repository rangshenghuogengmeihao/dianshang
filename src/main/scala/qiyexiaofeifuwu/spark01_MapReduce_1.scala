package qiyexiaofeifuwu

import org.apache.spark.{SparkConf, SparkContext}

object spark01_MapReduce_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Program Files\\hadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_MapReduce_1")
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
      line=>
      if (line.split(",").length >= theMax - 3){
        true
      } else {
        println(line)
        false
      }
    )
    println("=== “删除缺失值大于3个的字段的数据条数为"+(lines1.count()-result.count())+"条”===")
    result.coalesce(1).saveAsTextFile("src/main/resources/accommodation_output1")
    sc.stop()
  }
}
