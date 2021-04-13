package com.zozospider.flink.stream.source;

import com.zozospider.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Source - 自定义数据源
public class Source04UserDefinedSourceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据源
        DataStreamSource<SensorReading> dataStream = streamEnv.addSource(new MySourceFunction());

        dataStream.print();

        streamEnv.execute("Source");
    }

    // 自定义 SourceFunction
    static class MySourceFunction implements SourceFunction<SensorReading> {

        // 是否停止标识位
        private boolean running;

        @Override
        public void run(SourceContext<SensorReading> ctx) {
            running = true;

            // 模拟多个温度传感器, 按照高斯分布 (正态分布) 随机生成
            // 设置 10 个传感器的初始温度: id -> 温度
            Random random = new Random();
            HashMap<String, Double> sensorHashMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorHashMap.put("sensor" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            /* 方式 1
            while (running) {
                for (String id : sensorHashMap.keySet()) {
                    // 在当前温度基础上随机波动
                    double newTemperature = sensorHashMap.get(id) + random.nextGaussian();
                    sensorHashMap.put(id, newTemperature);
                    ctx.collect(new SensorReading(id, System.currentTimeMillis(), newTemperature));
                }
                Thread.sleep(3000);
            }*/

            // 方式 2
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                while (running) {
                    for (String id : sensorHashMap.keySet()) {
                        // 在当前温度基础上随机波动
                        double newTemperature = sensorHashMap.get(id) + random.nextGaussian();
                        sensorHashMap.put(id, newTemperature);
                        ctx.collect(new SensorReading(id, System.currentTimeMillis(), newTemperature));
                    }
                }
            }, 0, 3000, TimeUnit.MILLISECONDS);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
