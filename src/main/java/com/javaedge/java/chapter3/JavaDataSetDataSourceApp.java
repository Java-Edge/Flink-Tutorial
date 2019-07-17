package com.javaedge.java.chapter3;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author JavaEdge
 *
 * @date 2019-07-16
 */
public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
        textFile(env);
    }

    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Volumes/doc/data/data.txt";
        env.readTextFile(filePath).print();
        System.out.println("===========~这是一个分割线~============");

        filePath = "file:///Volumes/doc/data/inputs";
        env.readTextFile(filePath).print();
    }
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}
