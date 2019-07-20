package com.javaedge.scala.chapter4

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * @author JavaEdge
  * @date 2019-07-20
  */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long] {
  var isRunning = true
  var count = 1L

  override def cancel(): Unit = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }
}
