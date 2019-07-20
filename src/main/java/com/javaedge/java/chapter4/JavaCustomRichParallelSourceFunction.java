package com.javaedge.java.chapter4;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author JavaEdge
 * @date 2019-07-20
 */
public class JavaCustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {
    boolean isRunning = true;
    long count = 1;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (true) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
