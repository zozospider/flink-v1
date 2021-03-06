package com.zozospider.flink.stream.transform;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 转换算子 - 多流转换算子 - split() select() connect() coMap() union()
public class Transform04MultipleStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");

        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1. 分流:
        // tips: 当前新版本没有这个方法了, 也没有 SplitStream 这个类了

        // split():
        // DataStream → SplitStream: 根据某些特征把一个 DataStream 拆分成两个或者多个 DataStream
        // 按照温度分流

        // select():
        // SplitStream → DataStream: 从一个 SplitStream 中获取一个或者多个 DataStream

        SingleOutputStreamOperator<Sensor> lowTempDataStream = dataStream2.filter((Sensor sensorReading) -> sensorReading.getTemp() < 50);
        SingleOutputStreamOperator<Sensor> highTempDataStream = dataStream2.filter((Sensor sensorReading) -> sensorReading.getTemp() >= 50);
        lowTempDataStream.print("lowTempDataStream");
        highTempDataStream.print("highTempDataStream");

        // ------

        // 2. 合流:
        // collect():
        // DataStream, DataStream → ConnectedStreams: 连接两个保持他们类型的数据流
        // 两个数据流被 Connect 之后, 只是被放在了一个同一个流中, 内部依然保持各自的数据和形式不发生任何变化, 两个流相互独立
        // TODO 代码没写, 看不懂

        // union():
        // DataStream → DataStream: 对两个或者两个以上的 DataStream 进行 union 操作, 产生一个包含所有 DataStream 元素的新 DataStream
        DataStream<Sensor> dataStream3 = lowTempDataStream.union(highTempDataStream);
        dataStream3.print("dataStream3");

        streamEnv.execute("Transform");
    }

}
