package com.javaedge.java.chapter4;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author JavaEdge
 * @date 2019-07-20
 */
public class JavaCustomParallelSourceFunction implements ParallelSourceFunction<Long> {
    boolean isRunning = true;
    long count = 1;

    @Override
    public void run(SourceFunction.SourceContext<Long> ctx) throws Exception {
        while (true) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

