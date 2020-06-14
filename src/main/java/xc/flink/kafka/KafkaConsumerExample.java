/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xc.flink.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xc.flink.utils.SubmitUtils;

import java.util.Properties;

/**
 * An example of grouped stream windowing into sliding time windows. This
 * example uses [[RichParallelSourceFunction]] to generate a list of key-value
 * pairs.
 */
public class KafkaConsumerExample {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumerExample.class);

    public static void main(String[] args) throws Exception {
        //local environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //submit to remote environment
        String jarFile = SubmitUtils.getJarFile("flink-0.0.1-SNAPSHOT.jar");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.3.120", 8081, jarFile);

        env.setParallelism(1);
        //check point
        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.120:9092");
        //properties.setProperty("zookeeper.connect", "192.168.2.32:2181");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");


        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), properties);
        //FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test-json", new JSONKeyValueDeserializationSchema(true), properties);
        /**适用这个类型的Deserialization必须保证数据是json类型，并使用JsonNode来接收
         * {
         *   "value" : {
         *     "productId" : "产品1",
         *     "count" : 12
         *   },
         *   "metadata" : {
         *     "offset" : 0,
         *     "topic" : "test-json",
         *     "partition" : 0
         *   }
         * }
         */


        kafkaConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始

        /**
         * 在处理数据的过程中，若抛出异常则会导致task restart若这个异常是不可回避的那么该task会一直重启，所以在数据处理的过程中首先需要过滤掉脏数据
         * 使用map方法并添加try catch块捕获异常，使得后面的数据都是纯净的数据
         */
        DataStreamSink stream = env.addSource(kafkaConsumer).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String o) throws Exception {
                Tuple2<String, Integer> tuple2 = null;
                try {
                    JSONObject jsonObject = JSON.parseObject(o);
                    System.out.println(jsonObject);
                    tuple2 = new Tuple2<>(jsonObject.getString("productId"), jsonObject.getInteger("count"));
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
                return tuple2;
            }
        }).name("map-operation")
                .addSink(new SinkFunction() {
                    @Override
                    public void invoke(Object value, Context context) throws Exception {
                        if (value != null) {
                            log.info(value.toString());
                        }
                    }
                }).name("stdout-sink");

        env.execute("kafka-example");
    }
}
