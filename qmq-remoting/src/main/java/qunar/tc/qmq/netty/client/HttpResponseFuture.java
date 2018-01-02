package qunar.tc.qmq.netty.client;

import com.google.common.util.concurrent.AbstractFuture;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public class HttpResponseFuture<V> extends AbstractFuture<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseFuture.class);

    private final DefaultHttpResponse.DefaultHttpResponseBuilder builder = new DefaultHttpResponse.DefaultHttpResponseBuilder();
    private final HttpResponseCallback<V> callback;

    public HttpResponseFuture(HttpResponseCallback<V> callback) {
        this.callback = callback;
    }

    void onStatusAndHeader(HttpResponseStatus status, HttpHeaders headers) {
        builder.setStatus(status).setHeaders(headers);
    }

    void onContent(String body) {
        builder.appendBody(body);
    }

    void onCompleted() {
        try {
            V result = callback.onCompleted(builder.build());
            super.set(result);
        } catch (Exception e) {
            onThrowable(e);
        }
    }

    public void onThrowable(Throwable t) {
        try {
            callback.onThrowable(t);
        } catch (Exception e) {
            LOGGER.error("callback.onThrowable exception", e);
        }
        super.setException(t);
    }
}
