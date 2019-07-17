package com.javaedge.java.chapter3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * @author JavaEdge
 * @date 2019-07-17
 */
public class DataSetTransformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        mapFunction(env);
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map((MapFunction<Integer, Integer>) input -> input + 1).print();
    }
}
