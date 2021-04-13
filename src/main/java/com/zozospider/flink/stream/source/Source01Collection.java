package com.zozospider.flink.stream.source;

import com.zozospider.flink.beans.SensorReading;
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
        DataStreamSource<Integer> dataStreamA = streamEnv.fromElements(1, 2, 3, 4);
        DataStreamSource<SensorReading> dataStreamB = streamEnv.fromElements(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        );
        DataStreamSource<SensorReading> dataStreamC = streamEnv.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        dataStreamA.print("dataStreamA");
        dataStreamB.print("dataStreamB");
        dataStreamC.print("dataStreamC");

        streamEnv.execute("Source");
    }

}
