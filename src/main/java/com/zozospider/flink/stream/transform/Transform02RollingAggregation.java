package com.zozospider.flink.stream.transform;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 转换算子 - 滚动聚合算子 - keyBy() & sum() min() max() minBy() maxBy()
public class Transform02RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // keyBy():
        // 先通过 keyBy() 得到 KeyedStream 后才能做滚动聚合操作, 如 sum() min() 等

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");

        /*SingleOutputStreamOperator<SensorReading> dataStream2 = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });*/
        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(",");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // keyBy() 分组
        // KeyedStream<SensorReading, Tuple> dataStream3 = dataStream2.keyBy("id");
        /*KeyedStream<SensorReading, String> dataStream3 = dataStream2.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });*/
        /*KeyedStream<SensorReading, String> dataStream3 = dataStream2.keyBy((SensorReading sensorReading) ->
                sensorReading.getId());*/
        KeyedStream<Sensor, String> dataStream3 = dataStream2.keyBy(Sensor::getId);

        // 滚动聚合
        // 求最大的温度值
        SingleOutputStreamOperator<Sensor> dataStreamA = dataStream3.max("temperature");
        SingleOutputStreamOperator<Sensor> dataStreamB = dataStream3.maxBy("temperature");
        dataStreamA.print("dataStreamA");
        dataStreamB.print("dataStreamB");

        streamEnv.execute("Transform");
    }

}
