package xc.flink.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EventTimeWindow {

    public static final long TIME_INTERVAL = 5L;

    public static final long WATERMARK_STEP = 1L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, String, Integer>> dataStream = env.addSource(new DataSource());
        //根据lineId划分 每5秒统计一次 5秒内产品数量
        dataStream.keyBy(1)
                //.window(EventTimeSessionWindows.withGap(Time.milliseconds(TIME_INTERVAL)))
                .window(TumblingEventTimeWindows.of(Time.of(TIME_INTERVAL, TimeUnit.SECONDS)))
                .evictor(new Evictor<Tuple3<String, String, Integer>, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple3<String, String, Integer>>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple3<String, String, Integer>>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
                    }
                }).sum(2).addSink(new SinkFunction<Tuple3<String, String, Integer>>() {
            @Override
            public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
                System.out.println("[Result]Line: " + value.f1 + ", Count: " + value.f2);
                //输出后将数据清空
                value.f2 = 0;
            }
        });
        //.window(GlobalWindows.create());
        env.execute();
    }

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
            long startTime = System.currentTimeMillis();
            Random random = new Random(startTime);
            while (running) {
                Thread.sleep(WATERMARK_STEP * 1000);
                String producId = productIds[random.nextInt(productIds.length)];
                String lineId = lineIds[random.nextInt(lineIds.length)];
                int productCount = random.nextInt(10) + 1;
                Tuple3<String, String, Integer> element = new Tuple3<String, String, Integer>(producId, lineId,
                        productCount);
                System.out
                        .println("[Produce] Product : " + producId + ", Line :" + lineId + ", Count :" + productCount);

                //当前时间
                long currentTime = System.currentTimeMillis();
                //时间间隔（watermark）
                long period = currentTime - startTime;
                ctx.collectWithTimestamp(element, period);
                ctx.emitWatermark(new Watermark(period));
            }
            ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
        }
    }

}
