package com.zozospider.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Source - 从文件读取数据
public class Source02File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果设置并行度为 1, 则最终会顺序打印元素 (每个文件内部顺序打印)
        // streamEnv.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> dataStream = streamEnv.readTextFile("data-dir/word-count");

        dataStream.print();

        streamEnv.execute("Source");
    }

}
