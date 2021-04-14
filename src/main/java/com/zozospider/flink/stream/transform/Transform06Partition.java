package com.zozospider.flink.stream.transform;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 转换算子 - 分区 - keyBy() broadcast() shuffle() forward() rebalance() rescale() global() partitionCustom()
public class Transform06Partition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(4);

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");

        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        dataStream2.print("dataStream2");

        // keyBy():
        // 通过 key 的 hashcode 值进行分区
        KeyedStream<Sensor, String> dataStream3 = dataStream2.keyBy(Sensor::getId);
        dataStream3.print("dataStream3 keyBy()");

        // shuffle():
        // 随机分配
        DataStream<Sensor> dataStream4 = dataStream2.shuffle();
        dataStream4.print("dataStream4 shuffle()");

        // global():
        // 都分配到第一个分区
        DataStream<Sensor> dataStream5 = dataStream2.global();
        dataStream5.print("dataStream5 global()");

        streamEnv.execute("Transform");
    }


}
