package com.zozospider.flink.stream.source;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

// Source - 从集合读取数据
public class Source01Collection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果设置并行度为 1, 则最终会顺序打印元素
        // streamEnv.setParallelism(1);

        // 从集合读取数据
        DataStreamSource<Integer> dataStreamSourceA = streamEnv.fromElements(1, 2, 3, 4);
        DataStreamSource<Sensor> dataStreamSourceB = streamEnv.fromElements(
                new Sensor("id_1", 11111L, 11.1),
                new Sensor("id_6", 6666L, 66.6),
                new Sensor("id_7", 7777L, 77.7),
                new Sensor("id_9", 9999L, 99.9)
        );
        DataStreamSource<Sensor> dataStreamSourceC = streamEnv.fromCollection(Arrays.asList(
                new Sensor("id_1", 11111L, 11.1),
                new Sensor("id_6", 6666L, 66.6),
                new Sensor("id_7", 7777L, 77.7),
                new Sensor("id_9", 9999L, 99.9)
        ));

        dataStreamSourceA.print("dataStreamSourceA");
        dataStreamSourceB.print("dataStreamSourceB");
        dataStreamSourceC.print("dataStreamSourceC");

        streamEnv.execute("Source");
    }

}
