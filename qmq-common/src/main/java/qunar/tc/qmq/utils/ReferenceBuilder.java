package qunar.tc.qmq.utils;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * User: zhaohuiyu
 * Date: 12/25/12
 * Time: 11:41 AM
 */
public class ReferenceBuilder<T> {

    private final Class<T> interfaceClass;

    private String applicationOwner = null;
    private String applicationOrganization = null;

    private String registryAddress = "N/A";

    private String url = null;

    private String verson = "1.0.0";
    private Integer timeout = 3000;
    private static ReferenceConfig<?> referenceConfigForTest;

    public ReferenceBuilder(Class<T> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    public ReferenceBuilder<T> withApplicationOwner(String owner) {
        Preconditions.checkNotNull(owner);
        this.applicationOwner = owner;
        return this;
    }

    public ReferenceBuilder<T> withApplicationOrganization(String organization) {
        Preconditions.checkNotNull(organization);
        this.applicationOrganization = organization;
        return this;
    }

    public ReferenceBuilder<T> withRegistryAddress(String zkAddress) {
        Preconditions.checkNotNull(zkAddress);
        this.registryAddress = zkAddress;
        return this;
    }

    public ReferenceBuilder<T> withVersion(String version) {
        Preconditions.checkNotNull(version);
        this.verson = version;
        return this;
    }

    public ReferenceBuilder<T> withTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "time must >0 ");
        this.timeout = timeout;
        return this;
    }


    public ReferenceBuilder<T> withUrl(String url) {
        this.url = url;
        return this;
    }

    public ReferenceConfig<T> refer() {

        ApplicationConfig applicationConfig = new ApplicationConfig("QmQ");

        if (applicationOwner != null)
            applicationConfig.setOwner(applicationOwner);

        if (applicationOrganization != null)
            applicationConfig.setOrganization(applicationOrganization);

        RegistryConfig registryConfig = new RegistryConfig(registryAddress);

        ReferenceConfig<T> referenceConfig = new ReferenceConfig<T>();
        if (url != null) {
            referenceConfig.setUrl(url);
        }

        referenceConfig.setApplication(applicationConfig);
        referenceConfig.setRegistry(registryConfig);
        referenceConfig.setTimeout(timeout);
        referenceConfig.setVersion(verson);
        referenceConfig.setInterface(interfaceClass);
        Map<String, String> parameters = new HashMap<>();
        parameters.put(qunar.tc.qtracer.Constants.DUBBO_TRACE_SWITCH_KEY, "false");
        referenceConfig.setParameters(parameters);
        return referenceConfig;
    }

    public ReferenceConfig<T> build() {
        if (referenceConfigForTest != null) return (ReferenceConfig<T>) referenceConfigForTest;
        return refer();
    }

    public static <T> ReferenceBuilder<T> newRef(Class<T> interfaceClass) {
        return new ReferenceBuilder<T>(interfaceClass);
    }

    public static void __DONT_CALL_THIS_METHOD_JUST_FOR_TEST__(ReferenceConfig<?> refer) {
        referenceConfigForTest = refer;
    }
}
