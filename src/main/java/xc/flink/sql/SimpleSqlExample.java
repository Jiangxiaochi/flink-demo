package xc.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import xc.flink.sql.udf.Append;

public class SimpleSqlExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();//flink planner
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        TableEnvironment tenv = TableEnvironment.create(settings);

        tenv.registerFunction("append",new Append());

        /**
         * connectors
         * 1,file system
         * 2,Kafka
         * 3,HBase
         * 4,JDBC
         * 5,Hive
         */

        tenv.sqlUpdate("CREATE TABLE Products " +
                "(productId BIGINT, productName STRING, `count` INT)" +
                "WITH (" +
                "'connector.type' = 'kafka'," +
                "'connector.version' = '0.11'," +
                "'connector.topic' = 'test-sql'," +
                "'connector.startup-mode' = 'earliest-offset'," +
                "'connector.properties.zookeeper.connect' = '192.168.2.32:2181'," +
                "'connector.properties.bootstrap.servers' = '192.168.2.32:9092'," +
                "'connector.properties.group.id' = 'testGroup'," +
                "'update-mode' = 'append'," +//otherwise: 'retract' or 'upsert'
                "'format.type' = 'json')");//csv,json,avro

        //create another table
//        tenv.sqlUpdate("CREATE TABLE Outputs " +
//                "(productId BIGINT, productName STRING, `count` INT)" +
//                "WITH (" +
//                "'connector.type' = 'filesystem'," +
//                "'connector.path' = 'file:///D:/test-sql'," +
//                "'format.type' = 'csv')");

        tenv.sqlUpdate("CREATE TABLE Outputs " +
                "(productId BIGINT, productName STRING, `count` INT)" +
                "WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:postgresql://192.168.2.32:5432/flink-test?tinyInt1isBit=false'," +
                "'connector.table' = 'tb_test_sql'," +
                "'connector.driver' = 'org.postgresql.Driver'," +
                "'connector.username' = 'postgres'," +
                "'connector.password' = 'postgres'," +
                "'connector.write.flush.max-rows' = '1')");//默认5000条flush一次

        //insert into


        //tenv.insertInto("Outputs", result);
        tenv.sqlUpdate("INSERT INTO Outputs SELECT productId,append(productName,'verygood'),`count` FROM Products");

        tenv.execute("test-sql");
    }

}
