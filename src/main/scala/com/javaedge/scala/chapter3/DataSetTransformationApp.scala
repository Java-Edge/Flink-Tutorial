package com.javaedge.scala.chapter3

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @author JavaEdge
  * @date 2019-07-17
  *
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    mapFunction(env)
    filterFunction(env)
  }

  def filterFunction(env: ExecutionEnvironment): Unit = {
    //    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //    data.map(_ + 1).filter(_ > 5).print()

    env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .map(_ + 1)
      .filter(_ > 5)
      .print()
  }

  def mapFunction(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // data.print()
    // 对data中的元素都做加一处理
    // data.map((x: Int) => x + 1).print()
    // data.map((x) => x + 1).print()
    // data.map(x => x + 1).print()
    // 终极写法
    data.map(_ + 1).print()
  }
}
