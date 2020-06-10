package xc.flink.mongo.utils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.lang.reflect.InvocationTargetException;

public class MongoDBUtils {

    public static MongoClient mongoClient;

    public static synchronized void connect(String host, int port) {
        if (mongoClient == null) {
            synchronized (MongoDBUtils.class) {
                mongoClient = new MongoClient(host, port);
            }
        }
    }

    public static Document object2Document(Object o) throws IllegalAccessException {
        return new Document(ReflectUtils.object2Map(o));
    }

    public static <T> T document2Object(Document document, Class<T> clazz) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T t = clazz.newInstance();
        BeanUtils.populate(t, document);
        return t;
    }

    public static MongoDatabase getDataBase(String name) {
        if (StringUtils.isNotBlank(name)) {
            return mongoClient.getDatabase(name);
        }
        return null;
    }

    /**
     * user this type of collection should implement codec
     *
     * @param database
     * @param collectionName
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> MongoCollection<T> getCollection(MongoDatabase database, String collectionName, Class<T> clazz) {
        if (database != null && StringUtils.isNotBlank(collectionName)) {
            return database.getCollection(collectionName, clazz);
        }
        return null;
    }

    public static MongoCollection getCollection(MongoDatabase database, String collectionName) {
        if (database != null && StringUtils.isNotBlank(collectionName)) {
            return database.getCollection(collectionName);
        }
        return null;
    }

    public static void insert(MongoCollection collection, Object t) throws IllegalAccessException {
        if (collection != null && t != null) {
            collection.insertOne(object2Document(t));
        }
    }

    public static void updateOneBy(MongoCollection collection, String field, Object value, Object t) throws IllegalAccessException {
        collection.replaceOne(Filters.eq(field, value), object2Document(t));
    }

    public static void upsert(MongoCollection collection, String field, Object value, Object t) throws IllegalAccessException {
        collection.replaceOne(Filters.eq(field, value), object2Document(t), new UpdateOptions().upsert(true));
    }

    public static Document findOneBy(MongoCollection collection, String field, Object value) {
        return (Document) collection.find(Filters.eq(field, value)).first();
    }

    public static <T> T findOneBy(MongoCollection collection, String field, Object value, Class<T> clazz) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        Document document = findOneBy(collection, field, value);
        return document2Object(document, clazz);
    }

    public static void deleteOneBy(MongoCollection collection, String field, Object value) {
        collection.deleteOne(Filters.eq(field, value));
    }

}
