package com.example.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.util.Random


class CustomSource extends SourceFunction[Event] {
  private var running = true

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val user = List("Jack", "Tom", "Lucy", "Nicole", "Abel")
    val url = List("/home", "/cart", "/prod?id=3", "/pay", "/prod?id=2", "/prod?id=1")
    val ts = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli

    while (running) {
      ctx.collect(Event(user(Random.nextInt(user.length)), url(Random.nextInt(url.length)), ts))
      TimeUnit.SECONDS.sleep(2)
    }
  }

  override def cancel(): Unit = running = false
}
