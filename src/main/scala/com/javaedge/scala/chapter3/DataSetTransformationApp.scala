package com.javaedge.scala.chapter3

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * @author JavaEdge
  * @date 2019-07-17
  *
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    mapFunction(env)
    //    filterFunction(env)
    //    mapPartitionFunction(env)
    //    firstFunction(env)
    //    flatMapFunction(env)
    //    distinctFunction(env)
    //    joinFunction(env)
    //    outerJoinFunction(env)
    crossFunction(env)
  }

  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("曼联", "曼城")
    val info2 = List(3, 1, 0)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()
  }

  def outerJoinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "JavaEdge"));
    info1.append((2, "公众号"));
    info1.append((3, "全是干货"));
    info1.append((4, "进击架构师"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
      if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

    //    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
    //
    //      if(first == null) {
    //        (second._1, "-", second._2)
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()

    //    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
    //
    //      if (first == null) {
    //        (second._1, "-", second._2)
    //      } else if (second == null) {
    //        (first._1, first._2, "-")
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()
  }

  def joinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "JavaEdge"));
    info1.append((2, "公众号"));
    info1.append((3, "全是干货"));
    info1.append((4, "进击架构师"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }

  def distinctFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)
    data.flatMap(_.split(","))
      .distinct()
      .print()
  }

  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)

    data.flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }

  def firstFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Spring"))
    info.append((3, "Linux"))
    info.append((4, "React"))

    val data = env.fromCollection(info)
    //    data.first(3).print()
    data.groupBy(0).sortGroup(1, Order.ASCENDING)
      .first(2).print()
  }


  // DadaSource 100个元素,把结果存储到DB
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }

    val data = env.fromCollection(students).setParallelism(4)
    //    data.map(x => {
    //      // 每个元素都要存储到DB
    //      val connection = DBUtils.getConnection()
    //      println(connection + "...")
    //
    //      // TODO:保存数据到DB
    //      DBUtils.returnConnection(connection)
    //    }).print()

    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(connection + "...")
      DBUtils.returnConnection(connection)
      x
    }).print()
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
