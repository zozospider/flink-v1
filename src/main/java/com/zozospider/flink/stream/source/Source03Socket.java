package com.zozospider.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Source - 从 Socket 读取数据
public class Source03Socket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        dataStreamSource.print();

        streamEnv.execute();
    }

}
