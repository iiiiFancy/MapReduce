import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

object Task2 {
  var writer = new FileWriter(new File("Chang_Fan_result_task1.txt"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Please input file path and name")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster("local").setAppName("HW1_Q1")
    val sc = new SparkContext(conf)
    val ratings = sc.textFile(args(0))
    val users = sc.textFile(args(1))

    val ratings_3 = ratings.map(line => (line.split("::")(0).toInt, (line.split("::")(1), line.split("::")(2)))) //取前三个数据
    //ratings_3.collect().foreach { x => println(x)}
    val users_3 = users.map(line => (line.split("::")(0).toInt, (line.split("::")(1), line.split("::")(2)))) //取前三个
    //users_3.collect().foreach { x => println(x)}
    val R_U = ratings_3.join(users_3).sortByKey()
    //R_U.collect().foreach { x => println(x)}
    val Keys_are_Movie_Gender = R_U.map(f => (f._2._2._1, f._2._2._2, f._1, f._2._1._1, f._2._1._2)).map(f => ((f._4.toInt, f._1), f._5.toInt))
    //Keys_are_Movie_Gender.collect().foreach { x => println(x)}
    val Num_rate = Keys_are_Movie_Gender.map(f => (f._1, 1.0)).reduceByKey(_ + _) // 按key分别求和，对应每个电影评分个数
    //Num_rate.collect().foreach { x => println(x)}
    val Sum_rate = Keys_are_Movie_Gender.reduceByKey(_ + _)
    //Sum_rate.collect().foreach { x => println(x)}
    val MID_Gender_Sum_Num = Sum_rate.join(Num_rate)
    //MID_Gender_Sum_Num.collect().foreach { x => println(x)}
    val Avg = MID_Gender_Sum_Num.map(f => (f._1, f._2._1 / f._2._2)).sortByKey().map(f => (f._1._1, f._1._2, f._2))
    Avg.collect().foreach { x => println(x) }
    for (x <- Avg.collect()) {
      val str = x.toString
      val len = str.length
      val out = str.substring(1, len - 1)
      writer.write(out + System.lineSeparator())
    }
    writer.close()
  }
}