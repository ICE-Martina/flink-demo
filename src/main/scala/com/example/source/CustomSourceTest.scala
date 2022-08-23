package com.example.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


object CustomSourceTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

//    val sourceDs = environment.addSource(new CustomSource)
    val sourceDs = environment.addSource(new CustomParallelSource())

    sourceDs.print()

    environment.execute("CustomSourceTest")
  }

}
