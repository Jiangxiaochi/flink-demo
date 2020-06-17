package xc.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class Append extends ScalarFunction {

    public static String eval(String s1, String s2) {
        return s1.concat(s2);
    }

}
