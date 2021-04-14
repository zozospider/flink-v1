package com.zozospider.flink.stream.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Sink - 输出到 Socket
public class Sink01Socket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // TODO 未测试

        // 输出到 Socket
        DataStreamSource<Integer> dataStreamSource = streamEnv.fromElements(1, 2, 3, 4);
        // dataStreamSource.writeToSocket();

        streamEnv.execute("Sink");
    }

}
