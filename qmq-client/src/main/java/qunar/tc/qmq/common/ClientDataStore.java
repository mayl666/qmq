package qunar.tc.qmq.common;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import qunar.tc.qmq.utils.IPUtil;

import java.util.UUID;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class ClientDataStore extends LocalFileStore<ClientData> {
    private static final String FILE_NAME = "consumer.data";

    private static final ClientDataStore CONSUMER_DATA_STORE = new ClientDataStore();

    static {
        if (Strings.isNullOrEmpty(getClientId())) {
            throw new RuntimeException("init client id fail");
        }
    }

    public static String getClientId() {
        Optional<ClientData> opt = CONSUMER_DATA_STORE.load(true);
        return opt.isPresent() ? opt.get().getId() : "";
    }

    private ClientDataStore() {
        super(FILE_NAME, ClientData.class);
    }

    @Override
    protected ClientData create() {
        ClientData clientData = new ClientData();
        clientData.setId(IPUtil.getLocalHostName() + "-" + UUID.randomUUID().toString());
        return clientData;
    }
}
