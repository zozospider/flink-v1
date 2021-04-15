package com.zozospider.flink.window;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// Window - 计数窗口
public class Window02CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1. 分组:
        // .keyBy(Sensor::getId)
        // 2. 开窗:
        // .countWindow(10)
        // .countWindow(10, 2)
        // 3. 对窗口函数进行计算操作:
        // .reduce()
        // .aggregate()

        // 测试 A
        dataStream
                .keyBy(Sensor::getId)
                .countWindow(10, 2)
                .reduce((Sensor sensor1, Sensor sensor2) ->
                    sensor2.getTemp() > sensor1.getTemp() ? sensor2 : sensor1)
                .print();

        streamEnv.execute("Window");
    }

    // static class MyAggregateFunction extends AggregateFunction<> {}

}
