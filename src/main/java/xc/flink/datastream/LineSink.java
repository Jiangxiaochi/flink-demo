package xc.flink.datastream;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import xc.flink.utils.MongoDBUtils;

public class LineSink extends RichSinkFunction<Tuple3<String, String, Integer>> {

    MongoCollection collection;

    //Sink需要在open中实现资源初始化，不能在main()方法中传入
    @Override
    public void open(Configuration parameters) throws Exception {
        MongoDBUtils.connect("192.168.2.32", 27017);
        MongoDatabase database = MongoDBUtils.getDataBase("simicastool");
        collection = MongoDBUtils.getCollection(database, "statistic_data");
    }

    @Override
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
        StatisticData sd = new StatisticData();
        sd.setLineId(value.f1);
        sd.setCount(value.f2);
        MongoDBUtils.upsert(collection, "lineId", value.f1, sd);
    }

}
