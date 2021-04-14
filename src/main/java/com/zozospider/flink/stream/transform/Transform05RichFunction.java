package com.zozospider.flink.stream.transform;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 转换算子 - rich function 富函数
public class Transform05RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(2);

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");

        SingleOutputStreamOperator<Sensor> dataStream2 = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream3 = dataStream2.map(new MyRichMapFunction());
        dataStream3.print();

        streamEnv.execute("Transform");
    }

    // 富含数
    // 每个分区创建一个此对象
    static class MyRichMapFunction extends RichMapFunction<Sensor, Tuple2<String, Integer>> {

        @Override
        public void open(Configuration parameters) {
            // 初始化
            // 一般是定义状态, 或和外部系统做连接 (如数据库连接)
            System.out.println("open");
        }

        @Override
        public Tuple2<String, Integer> map(Sensor sensor) {
            // 获取运行环境等
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            return new Tuple2<>(sensor.getId(), indexOfThisSubtask);
        }

        @Override
        public void close() {
            // 关闭
            // 一般是关闭连接和清空状态等
            System.out.println("close");
        }
    }

}
