package com.example.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.util.Random


class CustomParallelSource extends ParallelSourceFunction[Event] {
  var running = true

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val user = List("Jack", "Tom", "Lucy", "Nicole", "Abel")
    val url = List("/home", "/cart", "/prod?id=3", "/pay", "/prod?id=2", "/prod?id=1")
    val ts = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli
    while (true) {
      ctx.collect(Event(user(Random.nextInt(user.length)), url(Random.nextInt(url.length)), ts))
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = running = false
}
