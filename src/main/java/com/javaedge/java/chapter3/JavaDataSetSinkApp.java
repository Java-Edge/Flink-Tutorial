package com.javaedge.java.chapter3;

/**
 * @author sss
 * @date 2019-07-18
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * @author JavaEdge
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> info = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            info.add(i);
        }

        String filePath = "/Volumes/doc/data/cp3/sink-out/";
        DataSource<Integer> data = env.fromCollection(info);
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        env.execute("JavaDataSetSinkApp");
    }
}
