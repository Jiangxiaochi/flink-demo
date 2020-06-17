package xc.flink.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class ReflectUtils {

    public static Map<String, Object> object2Map(Object o) throws IllegalAccessException {
        Class clazz = o.getClass();
        Map<String, Object> map = new HashMap<>();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields
        ) {
            field.setAccessible(true);
            map.put(field.getName(), field.get(o));
        }
        return map;
    }

}
