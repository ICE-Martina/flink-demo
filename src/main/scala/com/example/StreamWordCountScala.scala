package com.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWordCountScala {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val param = ParameterTool.fromArgs(args)
    // --host localhost --port 12345
    val host = param.get("host")
    val port = param.getInt("port")
    val sourceDs = environment.socketTextStream(host, port)

    val words = sourceDs.flatMap(_.split("\\W+")).map((_, 1))
    val result = words.keyBy(_._1).sum(1)

    result.print()
    environment.execute("StreamWordCount")
  }

}
