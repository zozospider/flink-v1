package com.zozospider.flink.stream.sink;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Sink - 输出到 Redis
// 参考: https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
public class Sink05Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/word-count");
        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        streamEnv.execute("Sink");
    }

}
