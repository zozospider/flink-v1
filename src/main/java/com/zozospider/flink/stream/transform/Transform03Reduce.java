package com.zozospider.flink.stream.transform;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 转换算子 - 聚合算子 - keyBy() & reduce()
public class Transform03Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // keyBy():
        // 先通过 keyBy() 得到 KeyedStream 后才能做聚合操作, 如 reduce()

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");

        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // keyBy() 分组
        KeyedStream<Sensor, String> dataStream3 = dataStream2.keyBy(Sensor::getId);

        // reduce() 聚合
        // 求最大的温度值, 时间戳用当前数据的
        /*SingleOutputStreamOperator<SensorReading> dataStream4 = dataStream3.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading1, SensorReading sensorReading2) throws Exception {
                return new SensorReading(
                        sensorReading1.getId(),
                        sensorReading2.getTimestamp(),
                        Math.max(sensorReading1.getTemperature(), sensorReading2.getTemperature()));
            }
        });*/
        SingleOutputStreamOperator<Sensor> dataStream4 = dataStream3.reduce((Sensor value1, Sensor value2) ->
                new Sensor(
                        value1.getId(),
                        value2.getTimestamp(),
                        Math.max(value1.getTemperature(), value2.getTemperature()))
        );
        dataStream4.print();

        streamEnv.execute("Transform");
    }

}
