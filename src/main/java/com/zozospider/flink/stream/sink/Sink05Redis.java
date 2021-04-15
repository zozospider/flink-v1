package com.zozospider.flink.stream.sink;

import com.zozospider.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

// Sink - 输出到 Redis
// 参考: https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
public class Sink05Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 未测试

        DataStreamSource<String> dataStreamSource = streamEnv.readTextFile("data-dir/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = dataStreamSource.map((String s) -> {
            String[] fields = s.split(" ");
            return new Sensor(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        dataStream.addSink(new RedisSink<>(flinkJedisConfigBase, new MyRedisMapper()));

        streamEnv.execute("Sink");

        // 要查看 Redis 结果, 可尝试执行以下命令 (未测试):
        // > redis-cli
        // keys *
        // hgetall sensor_temp
        // hget sensor_temp id_1
    }

    // 自定义 RedisMapper
    static class MyRedisMapper implements RedisMapper<Sensor> {

        // 存成 hash 表
        // 表名: hset
        // 表名: sensor_temp
        // 键值对: id -> temperature

        // 定义保存数据到 Redis 的命令和表名
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(Sensor sensor) {
            return sensor.getId();
        }

        @Override
        public String getValueFromData(Sensor sensor) {
            return sensor.getTemperature().toString();
        }
    }

}
