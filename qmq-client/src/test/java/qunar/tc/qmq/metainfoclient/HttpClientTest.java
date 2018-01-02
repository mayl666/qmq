package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.netty.client.HttpClient;
import qunar.tc.qmq.netty.client.HttpResponseCallback;
import qunar.tc.qmq.netty.client.Response;

/**
 * @author yiqun.fan create on 17-8-30.
 */
public class HttpClientTest {
    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClient.newClient();
        client.get("http://meta.newqmq.dev.qunar.com/meta/address", null,
                new HttpResponseCallback<Object>() {
                    @Override
                    public Object onCompleted(Response response) throws Exception {
                        System.out.println(response.getStatusCode() + ", " + response.getBody());
                        return null;
                    }

                    @Override
                    public void onThrowable(Throwable t) {
                        t.printStackTrace();
                    }
                }).get();
        client.shutdown();
    }
}
