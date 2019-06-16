package com.javaedge.scala.chapter1

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author JavaEdge
  * @date 2019-05-04
  *
  */
object StreamingWCApp {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    StreamExecutionEnvironment.createLocalEnvironment()

    // 引入隐式转换
    import org.apache.flink.api.scala._

    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }
}


