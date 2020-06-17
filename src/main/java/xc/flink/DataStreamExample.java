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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class DataStreamExample {

    public static final Logger log = LoggerFactory.getLogger(DataStreamExample.class);

    public static void main(String[] args) throws Exception {
        //local environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //submit to remote environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.2.32", 8083, "D:/Repository/flink-demo/target/flink-0.0.1-SNAPSHOT.jar");

        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> stream = env.addSource(new DataSource());

        // 按产线分组计算结果
        stream.keyBy(1).sum(2).addSink(new LineSink());

        // 按产品分组计算结果（混线）
        stream.keyBy(0).sum(2).addSink(new ProductSink());

        // 按产线和产品分组计算结果
        stream.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {

            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0 + value.f1;
            }
        }).sum(2).addSink(new SinkFunction<Tuple3<String, String, Integer>>() {
            @Override
            public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
                log.info("[Line&Product] Product : " + value.f0 + ", Line :" + value.f1 + ", Count :" + value.f2);
            }
        });

        // 所有产线所有产品总和
        stream.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {

            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return "";
            }
        }).sum(2).addSink(new SinkFunction<Tuple3<String, String, Integer>>() {
            @Override
            public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
                log.info("[Total] Count :" + value.f2);
            }
        });

        env.execute();
    }

    // private static class DataSource extends RichParallelSourceFunction<Tuple3<String, String,Integer>> {
    private static class DataSource extends RichSourceFunction<Tuple3<String, String, Integer>> {

        private final int THREAD_COUNT = 20;

        private volatile boolean running = true;

        private final String[] productIds = new String[]{"产品1", "产品2", "产品3"};

        private final String[] lineIds = new String[]{"产线1", "产线2"};

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
            for (int i = 0; i < THREAD_COUNT; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Random random = new Random(System.currentTimeMillis());
                        while (running) {
                            try {
                                Thread.currentThread().sleep(50);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            String producId = productIds[random.nextInt(productIds.length)];
                            String lineId = lineIds[random.nextInt(lineIds.length)];
                            int productCount = random.nextInt(10) + 1;
                            Tuple3<String, String, Integer> element = new Tuple3<String, String, Integer>(producId, lineId,
                                    productCount);
                            log.info("[Produce] Product : " + producId + ", Line :" + lineId + ", Count :" + productCount);
                            ctx.collect(element);
                        }
                    }
                }).start();
            }
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
