package xc.flink.utils;

import java.io.File;
import java.io.IOException;

public class SubmitUtils {

    public static String getJarFile(String name) throws IOException {
        return new File("target/" + name).getCanonicalPath();
    }

}
