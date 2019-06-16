package com.javaedge.scala.chapter1

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @author JavaEdge
  * @date 2019-05-04
  *
  */
object BatchWCApp {

  def main(args: Array[String]): Unit = {

    val input = "/Volumes/doc/tmp/flink/input"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._
    text.flatMap (_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

  }
}
