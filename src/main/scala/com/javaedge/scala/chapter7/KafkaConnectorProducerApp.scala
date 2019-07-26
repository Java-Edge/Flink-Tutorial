package com.javaedge.scala.chapter7

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper


/**
  * @author JavaEdge
  * @date 2019-07-26
  */
object KafkaConnectorProducerApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 常用检查点设置参数
    env.enableCheckpointing(4000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 从socket接收数据，通过Flink将数据Sink到Kafka
    val data = env.socketTextStream("localhost", 9999)

    val topic = "testJavaEdge"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.106:9092")

    //    val kafkaSink = new FlinkKafkaProducer[String](topic,
    //      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
    //      properties)
    val kafkaSink = new FlinkKafkaProducer[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      properties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    data.addSink(kafkaSink)

    env.execute("KafkaConnectorProducerApp")
  }
}

