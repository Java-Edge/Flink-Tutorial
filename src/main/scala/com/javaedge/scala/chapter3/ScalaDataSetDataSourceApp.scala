package com.javaedge.scala.chapter3

import com.javaedge.java.chapter3.Person
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * @author JavaEdge
  * @date 2019-07-16
  */
object ScalaDataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    fromCollection(env)
    //    textFile(env)
    //    csvFile(env)
    readCompressionFiles(env)
  }

  def readCompressionFiles(env:ExecutionEnvironment): Unit ={
    val filePath = "file:///Volumes/doc/data/compress"
    env.readTextFile(filePath).print()
  }

  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    // ❌
    val filePath = "file:///Volumes/doc/data/nested"
    env.readTextFile(filePath).print()
    println("===========~这是一个分割线~============")

    // ✔️
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameters).print()
  }

  def csvFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val filePath = "file:///Volumes/doc/data/data.csv"
    //    env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()
    //    env.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true,includedFields = Array(0, 1)).print()

    /*无法正常运行
      case class MyCaseClass(name: String, age: Int)
      env.readCsvFile[MyCaseClass](filePath,ignoreFirstLine = true,includedFields = Array(0, 1)).print()
    */
    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "job")).print()
  }

  def textFile(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Volumes/doc/data/inputs"
    env.readTextFile(filePath).print()
  }

  def fromCollection(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }
}
