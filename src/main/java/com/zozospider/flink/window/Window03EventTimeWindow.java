package com.zozospider.flink.window;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

// Window - 事件时间
// 参考: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_time.html
// 参考: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_timestamps_watermarks.html
public class Window03EventTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        // Flink 1.12 后 time characteristic 默认为 EventTime, 所以不需要单独设置
        // streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置周期性生成 watermark 时间
        streamEnv.getConfig().setAutoWatermarkInterval(500);

        DataStreamSource<String> dataStreamSource = streamEnv.socketTextStream("localhost", 7777);
        dataStreamSource.print();
        SingleOutputStreamOperator<Sensor> dataStream = dataStreamSource.flatMap((String s, Collector<Sensor> out) -> {
            String[] fields = s.split(" ");
            out.collect(new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])));
        }).returns(Types.POJO(Sensor.class));

        // 设置事件时间戳和 watermark
        SingleOutputStreamOperator<Sensor> dataStream2 = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((Sensor element, long recordTimestamp) -> element.getTime() * 1000L));

        dataStream2
                .keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(6)))
                .minBy("temp")
                .print();

        streamEnv.execute("Window");
    }

}
