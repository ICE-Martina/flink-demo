package com.example

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

import java.nio.charset.StandardCharsets

/**
 * DataSet api
 * @author liuwei 2022/8/19 9:49
 * @return
 */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val source = environment.readTextFile("./input/paper.txt", StandardCharsets.UTF_8.name())
    val words = source.flatMap(_.split("\\W+")).map((_, 1))
    val result = words.groupBy(0).sum(1)
    result.print()
  }
}
