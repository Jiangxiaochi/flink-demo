package xc.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class SimpleSqlExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();//flink planner
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        TableEnvironment tenv = TableEnvironment.create(settings);

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
                //
                "'connector.type' = 'kafka'," +
                "'connector.version' = '0.11'," +
                "'connector.topic' = 'test-sql'," +
                "'connector.startup-mode' = 'earliest-offset'," +
                "'connector.properties.bootstrap.servers' = '192.168.3.120:9092'," +
                //
                "'update-mode' = 'append'," +//otherwise: 'retract' or 'upsert'
                //
                "'format.type' = 'json')");//csv,json,avro

        Table result = tenv.sqlQuery("SELECT * FROM Products");

        //create another table
        tenv.sqlUpdate("CREATE TABLE Outputs " +
                "(productId BIGINT, productName STRING, `count` INT)" +
                "WITH (" +
                "'connector.type' = 'filesystem'," +
                "'connector.path' = 'file:///D:/test.txt'," +
                "'format.type' = 'csv')");
        //insert into

        tenv.insertInto("Outputs", result);

        tenv.execute("test-sql");
    }

}
