package com.example

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.readTextFile("./input/paper.txt")
    val words = source.flatMap(_.split("\\W+")).map((_, 1))
    val result = words.keyBy(_._1).sum(1)

    result.print()

    environment.execute("BoundedStreamWordCount")
  }

}
