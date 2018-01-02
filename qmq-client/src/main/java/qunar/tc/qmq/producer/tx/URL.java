/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qunar.tc.qmq.producer.tx;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class URL implements Serializable {

    private static final long serialVersionUID = -1985165475234910535L;

    private final String protocol;

    private final String host;

    private final int port;

    private String path;

    private final Map<String, String> parameters;

    public URL(String protocol, String host, int port, String path, Map<String, String> parameters) {
        this.protocol = protocol;
        this.host = host;
        this.port = (port < 0 ? 0 : port);
        // trim the beginning "/"
        while (path != null && path.startsWith("/")) {
            path = path.substring(1);
        }
        if (parameters == null) {
            parameters = new HashMap<String, String>();
        } else {
            parameters = new HashMap<String, String>(parameters);
        }
        this.parameters = Collections.unmodifiableMap(parameters);
    }

    /**
     * Parse url string
     *
     * @param url URL string
     * @return URL instance
     * @see URL
     */
    public static URL valueOf(String url) {
        if (url == null || (url = url.trim()).length() == 0) {
            throw new IllegalArgumentException("url == null");
        }

        Map<String, String> parameters = null;
        int i = url.indexOf("?"); // seperator between body and parameters
        if (i >= 0) {
            parameters = parseParameters(url, i);
            url = url.substring(0, i);
        } else {
            i = url.indexOf(';');
            if (i > 0) {
                parameters = parseParameters(url, i);
                url = url.substring(0, i);
            }
        }

        String protocol = null;
        i = url.indexOf("://");
        if (i >= 0) {
            if (i == 0) throw new IllegalStateException("url missing protocol: \"" + url + "\"");
            protocol = url.substring(0, i);
            url = url.substring(i + 3);
        } else {
            // case: file:/path/to/file.txt
            i = url.indexOf(":/");
            if (i >= 0) {
                if (i == 0) throw new IllegalStateException("url missing protocol: \"" + url + "\"");
                protocol = url.substring(0, i);
                url = url.substring(i + 1);
            }
        }

        String path = null;
        i = url.indexOf("/");
        if (i >= 0) {
            path = url.substring(i + 1);
            url = url.substring(0, i);
        }

        int port = 0;
        i = url.indexOf(":");
        if (i >= 0 && i < url.length() - 1) {
            port = Integer.parseInt(url.substring(i + 1));
            url = url.substring(0, i);
        }

        String host = null;
        if (url.length() > 0) host = url;
        return new URL(protocol, host, port, path, parameters);
    }

    private static Map<String, String> parseParameters(String url, int i) {
        String parameterParts = url.substring(i + 1);
        Map<String, String> parameters = parseWith(parameterParts, "\\&");
        if (parameters.isEmpty()) {
            parameters = parseWith(parameterParts, ";");
        }
        return parameters;
    }

    private static Map<String, String> parseWith(String input, String seperator) {
        Map<String, String> parameters = new HashMap<String, String>();
        String[] parts = input.split(seperator);
        if (parts.length == 1) return parameters;

        for (String part : parts) {
            part = part.trim();
            if (part.length() > 0) {
                int j = part.indexOf('=');
                if (j >= 0) {
                    parameters.put(part.substring(0, j), part.substring(j + 1));
                } else {
                    parameters.put(part, part);
                }
            }
        }
        return parameters;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return this.path;
    }

    public int getPort(int defaultPort) {
        return port <= 0 ? defaultPort : port;
    }

    public String getAddress() {
        return port <= 0 ? host : host + ":" + port;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getParameter(String key) {
        return parameters.get(key);
    }

    public static void main(String[] args) {
        URL url = URL.valueOf("jdbc:sqlserver://HtlOrderShard01.db.fat.qa.nt.ctripcorp.com:55111;authenticationScheme=nativeAuthentication;xopenStates=false;sendTimeAsDatetime=true;trustServerCertificate=false;sendStringParametersAsUnicode=true;selectMethod=direct;responseBuffering=adaptive;packetSize=8000;multiSubnetFailover=false;loginTimeout=15;lockTimeout=-1;lastUpdateCount=true;encrypt=false;disableStatementPooling=true;databaseName=HtlOrderShard01DB;applicationName=Microsoft JDBC Driver for SQL Server;applicationIntent=readwrite;");
        System.out.println(url);
    }


}