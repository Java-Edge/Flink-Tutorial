package com.javaedge.scala.chapter7

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * @author JavaEdge
  * @date 2019-07-23
  */
object FileSystemSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val filePath = "file:///Volumes/doc/data/HDFSSink"
    val sink = new BucketingSink[String](filePath)

    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
    sink.setBatchRolloverInterval(2000)

    data.addSink(sink)
    env.execute("FileSystemSinkApp")
  }
}

