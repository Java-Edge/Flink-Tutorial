package com.javaedge.java.chapter5;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author JavaEdge
 * @date 2019-07-21
 */
public class JavaTableSQLAPI {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        String filePath = "file:///Volumes/doc/IDEAProjects/Flink-Tuitiorial/flink-train-scala/src/main/resources/sales.csv";

        DataSet<Sales> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(Sales.class, "transactionId", "customerId", "itemId", "amountPaid");
        //csv.print();

        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales", sales);
        Table resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");

        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();
    }

    public static class Sales {
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double amountPaid;
    }
}

