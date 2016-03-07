package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Mutual {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("mutual")
        val sc = new SparkContext(conf)

        val userA = args(0)
        val userB = args(1)

        val input = sc.textFile("input.txt");
        val filteredInput = input.filter(line => (line.contains(userA + "\t") || line.contains(userB + "\t"))).map(line => line.split("\t")).flatMap(line => line(1).split(",")).countByValue().filter(line => (line._2 > 1)).map(line => line._1).foreach(println);
        

    }
}