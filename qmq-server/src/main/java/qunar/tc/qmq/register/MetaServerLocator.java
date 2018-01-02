package qunar.tc.qmq.register;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author keli.wang
 * @since 2017/9/1
 */
public class MetaServerLocator {
    private static final Logger LOG = LoggerFactory.getLogger(MetaServerLocator.class);

    private static final Splitter ADDRESS_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private static final AsyncHttpClient CLIENT = new AsyncHttpClient();

    private final String metaServerEndpoint;


    public MetaServerLocator(final String metaServerEndpoint) {
        this.metaServerEndpoint = metaServerEndpoint;
    }

    public ImmutableList<String> getMetaServerEndpoints() {
        return requestMetaServerAddresses();
    }

    private ImmutableList<String> requestMetaServerAddresses() {
        try {
            final Response resp = CLIENT.prepareGet(metaServerEndpoint).execute().get();
            if (resp.getStatusCode() == HttpResponseStatus.OK.code()) {
                return ImmutableList.copyOf(ADDRESS_SPLITTER.split(resp.getResponseBody()));
            }
        } catch (Exception e) {
            LOG.error("request meta server addresses failed, return empty address list. endpoint: {}", metaServerEndpoint, e);
        }

        return ImmutableList.of();
    }
}
