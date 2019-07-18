package com.javaedge.scala.chapter3

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._
/**
  * @author JavaEdge
  *
  * @date 2019-07-18
  */
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1.to(10)
    val text = env.fromCollection(data)

    val filePath = "/Volumes/doc/data/cp3/sink-out/"
    text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(5)
    env.execute("DataSetSinkApp")
  }
}
