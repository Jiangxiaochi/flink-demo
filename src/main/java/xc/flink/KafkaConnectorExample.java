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

package xc.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

/**
 * An example of grouped stream windowing into sliding time windows. This
 * example uses [[RichParallelSourceFunction]] to generate a list of key-value
 * pairs.
 */
public class KafkaConnectorExample {

    public static final Logger log = LoggerFactory.getLogger(KafkaConnectorExample.class);

    public static void main(String[] args) throws Exception {
        //local environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //submit to remote environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.2.32", 8083, "D:/Repository/flink-demo/target/flink-0.0.1-SNAPSHOT.jar");
        env.setParallelism(4);
        //check point
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties properties = new Properties();
        //ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
        //ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.32:9092");
        //properties.setProperty("zookeeper.connect", "192.168.2.32:2181");
        //properties.setProperty("group.id", "test-consumer-group");

        //FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), properties);
//        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("test", new TypeInformationSerializationSchema(TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {
//        }), new ExecutionConfig()), properties);

        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("test", new TypeInformationSerializationSchema(TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {
        }), new ExecutionConfig()), properties);


        //DataStream<Tuple3<String, String, Integer>> stream = env.addSource(new DataSource());
        DataStreamSink stream = env.addSource(new DataSource()).addSink(kafkaProducer);

        env.execute();
    }

    /**
     * Parallel data source that serves a list of key-value pairs.
     */
    // private static class DataSource extends
    // RichParallelSourceFunction<Tuple3<String, String,Integer>> {
    private static class DataSource extends RichSourceFunction<Tuple3<String, String, Integer>> {

        private volatile boolean running = true;

        private final String[] productIds = new String[]{"产品1", "产品2", "产品3"};

        private final String[] lineIds = new String[]{"产线1", "产线2"};

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
            Random random = new Random(System.currentTimeMillis());
            while (running) {
                Thread.sleep(100);
                String producId = productIds[random.nextInt(productIds.length)];
                String lineId = lineIds[random.nextInt(lineIds.length)];
                int productCount = random.nextInt(10) + 1;
                Tuple3<String, String, Integer> element = new Tuple3<String, String, Integer>(producId, lineId,
                        productCount);
                log.info("[Produce] Product : " + producId + ", Line :" + lineId + ", Count :" + productCount);
//                System.out
//                        .println("[Produce] Product : " + producId + ", Line :" + lineId + ", Count :" + productCount);
                ctx.collect(element);
            }
        }
    }
}
