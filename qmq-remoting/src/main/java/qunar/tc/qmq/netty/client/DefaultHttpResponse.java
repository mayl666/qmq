package qunar.tc.qmq.netty.client;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author yiqun.fan create on 17-8-29.
 */
class DefaultHttpResponse implements Response {
    private final HttpResponseStatus status;
    private final HttpHeaders headers;
    private final String body;

    DefaultHttpResponse(HttpResponseStatus status, HttpHeaders headers, String body) {
        this.status = status;
        this.headers = headers;
        this.body = body;
    }

    @Override
    public int getStatusCode() {
        return status.code();
    }

    @Override
    public String getHeader(String name) {
        return headers.get(name);
    }

    @Override
    public String getBody() {
        return body;
    }

    static final class DefaultHttpResponseBuilder {
        private volatile HttpResponseStatus status;
        private volatile HttpHeaders headers;
        private final StringBuilder body = new StringBuilder();

        DefaultHttpResponseBuilder setStatus(HttpResponseStatus status) {
            this.status = status;
            return this;
        }

        DefaultHttpResponseBuilder setHeaders(HttpHeaders headers) {
            this.headers = headers;
            return this;
        }

        DefaultHttpResponseBuilder appendBody(String body) {
            this.body.append(body);
            return this;
        }

        Response build() {
            return new DefaultHttpResponse(status, headers, body.toString());
        }
    }
}
