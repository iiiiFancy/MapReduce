import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

object Task1 {
  var writer2 = new FileWriter(new File("Chang_Fan_result_task2.txt"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Please input file path and name")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster("local").setAppName("HW1_Q2")
    val sc = new SparkContext(conf)
    val ratings = sc.textFile(args(0))
    val users = sc.textFile(args(1))
    val movies = sc.textFile(args(2))

    val ratings_3 = ratings.map(line => (line.split("::")(0).toInt, (line.split("::")(1), line.split("::")(2)))) //取前三个数据
    //ratings_3.collect().foreach { x => println(x)}
    val users_3 = users.map(line => (line.split("::")(0).toInt, (line.split("::")(1), line.split("::")(2)))) //取前三个
    //users_3.collect().foreach { x => println(x)}
    val movies_2 = movies.map(line => (line.split("::")(0).toInt, line.split("::")(2))) //取前一三
    //users_3.collect().foreach { x => println(x)}
    val R_U = ratings_3.join(users_3).sortByKey()
    //R_U.collect().foreach { x => println(x)}
    val Key_is_MID = R_U.map(f => (f._2._1._1.toInt, (f._2._2._1, f._2._2._2, f._1, f._2._1._2)))
    //Key_is_MID.collect().foreach { x => println(x)}
    val R_U_M = movies_2.join(Key_is_MID).sortByKey()
    //R_U_M.collect().foreach { x => println(x)}
    val Keys_are_Genre_Gender = R_U_M.map(f => ((f._2._1, f._2._2._1), f._2._2._4.toInt)).sortByKey()
    //Keys_are_Genre_Gender.collect().foreach { x => println(x)}
    val Sum_rate = Keys_are_Genre_Gender.reduceByKey(_ + _)
    //Sum_rate.collect().foreach { x => println(x)}
    val Num_rate = Keys_are_Genre_Gender.map(f => (f._1, 1.0)).reduceByKey(_ + _)
    //Num_rate.collect().foreach { x => println(x)}
    val Gen_Sum_Num = Sum_rate.join(Num_rate)
    //Gen_Sum_Num.collect().foreach { x => println(x)}
    val Avg = Gen_Sum_Num.map(f => (f._1, f._2._1 / f._2._2)).sortByKey().map(f => (f._1._1, f._1._2, f._2))
    //Avg.collect().foreach { x => println(x) }

    for (x <- Avg.collect()) {
      val str = x.toString
      val len = str.length
      val out = str.substring(1, len - 1)
      writer2.write(out + System.lineSeparator())
    }
    writer2.close()
  }
}