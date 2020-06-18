package xc.flink.submit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.IOException;

public class HttpSubmit {

    public static void main(String[] args) {
        //submitJar("192.168.2.32:8083", "D:/Repository/flink-demo/target/flink-0.0.1-SNAPSHOT.jar", "flink-0.0.1-SNAPSHOT.jar");
        run("192.168.2.32:8083", "11b56beb-7c7c-4a37-9c6b-f72f36e10628_flink-0.0.1-SNAPSHOT.jar");
    }

    public static void submitJar(String server, String file, String name) {
        CloseableHttpClient client = HttpClients.createDefault();
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("jarfile", new File(file), ContentType.create("application/x-java-archive"), name);
        HttpEntity entity = builder.build();
        //HttpEntity entity = builder.build();
        HttpPost httpPost = new HttpPost("http://" + server + "/jars/upload");
        httpPost.setEntity(entity);
        HttpResponse httpResponse = null;
        try {
            httpResponse = client.execute(httpPost);
            System.out.println(httpResponse.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    public static List<JarInfo> getJars(String server){
//        CloseableHttpClient client = HttpClients.createDefault();
//        HttpPost httpPost = new HttpPost("http://" + server + "/jars/" + jarId + "/run");
//        HttpResponse httpResponse = null;
//        try {
//            httpResponse = client.execute(httpPost);
//            System.out.println(httpResponse.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (client != null) {
//                try {
//                    client.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

    public static void run(String server, String jarId) {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("http://" + server + "/jars/" + jarId + "/run");
        HttpResponse httpResponse = null;
        try {
            httpResponse = client.execute(httpPost);
            System.out.println(httpResponse.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
