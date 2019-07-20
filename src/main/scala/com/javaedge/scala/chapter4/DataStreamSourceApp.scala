package com.javaedge.scala.chapter4

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @author JavaEdge
  * @date 2019-07-19
  */
object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // socketFunction(env)
    nonParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print().setParallelism(1)
  }

  def socketFunction(env: StreamExecutionEnvironment): Unit = {

    val data = env.socketTextStream("localhost", 9999)
    data.print().setParallelism(1)
  }

  //  def richParallelSourceFunction(env:StreamExecutionEnvironment): Unit = {
  //    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(2)
  //    data.print()
  //  }
  //
  //  def parallelSourceFunction(env:StreamExecutionEnvironment): Unit = {
  //    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
  //    data.print()
  //  }
  //
  //

}

