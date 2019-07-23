package com.javaedge.scala.chapter5

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

/**
  * @author JavaEdge
  * @date 2019-07-21
  *
  */
object TableSQLAPI {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "file:///Volumes/doc/IDEAProjects/Flink-Tuitiorial/flink-train-scala/src/main/resources/sales.csv"

    // 已经拿到DataSet
    val csv = env.readCsvFile[SalesLog](filePath, ignoreFirstLine = true)
    //csv.print()

    // DataSet ==> Table
    val salesTable = tableEnv.fromDataSet(csv)

    // Table ==> table
    tableEnv.registerTable("sales", salesTable)

    // SQL
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()

  }

  case class SalesLog(transactionId: String,
                      customerId: String,
                      itemId: String,
                      amountPaid: Double)

}
