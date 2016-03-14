package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.matching.Regex

object Mutual {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("mutual")
        val sc = new SparkContext(conf)

        val userA = args(1)
        val userB = args(2)

        val input = sc.textFile(args(0));
        val userAFriends = input.filter(line => line.startsWith(userA + "\t")).flatMap(line => line.split("\t")(1).split(","))
        val userBFriends = input.filter(line => line.startsWith(userB + "\t")).flatMap(line => line.split("\t")(1).split(","))

        val unionOfFriends = userAFriends.union(userBFriends)
        val mutualFriends = unionOfFriends.map(friend => (friend, 1)).reduceByKey(_ + _).filter(friendCount => (friendCount._2 == 2)).map(friendCount => friendCount.swap).reduceByKey(_ + "," + _).map(countFriend => (userA + "," + userB + "\t" + countFriend._2))
        mutualFriends.saveAsTextFile(".\\output")
    }
}