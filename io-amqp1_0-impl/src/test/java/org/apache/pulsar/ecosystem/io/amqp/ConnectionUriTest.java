/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.amqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;



/**
 * ConnectionUriTest.
 */
public class ConnectionUriTest {

    @Test
    public void validateTestConnectionUriWithHostEmpty() throws Exception {
        Map<String, Object> paramsMap = getConnectionUriConfig("", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true"));

        ConnectionUri connectionUri = ConnectionUri.load(paramsMap);

        try {
            connectionUri.validate();
            Assert.fail("The test should fail because connection are incomplete");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for "
                    + "all uris in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestConnectionUriWithProtocolEmpty() throws Exception {
        Map<String, Object> paramsMap = getConnectionUriConfig("host", 5672, "",
                Arrays.asList("transport.tcpKeepAlive=true"));

        ConnectionUri connectionUri = ConnectionUri.load(paramsMap);

        try {
            connectionUri.validate();
            Assert.fail("The test should fail because connection are incomplete");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for "
                    + "all uris in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestConnectionUriWithPortLessThanOne() throws Exception {
        Map<String, Object> paramsMap = getConnectionUriConfig("test", 0, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true"));

        ConnectionUri connectionUri = ConnectionUri.load(paramsMap);

        try {
            connectionUri.validate();
            Assert.fail("The test should fail because connection are incomplete");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("Protocol, host and port should be set for "
                    + "all uris in connection", e.getMessage());
        }
    }

    @Test
    public void validateTestUriBasedOnConnection() throws Exception {
        Map<String, Object> paramsMap = getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true"));

        ConnectionUri connectionUriConfig = ConnectionUri.load(paramsMap);
        connectionUriConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true", connectionUriConfig.getUri());
    }

    @Test
    public void validateTestUriBasedOnConnectionWithMultipleUrlOptions() throws Exception {
        Map<String, Object> paramsMap = getConnectionUriConfig("localhost", 5672, "amqp",
                Arrays.asList("transport.tcpKeepAlive=true", "extra_property=false"));

        ConnectionUri connectionUriConfig = ConnectionUri.load(paramsMap);
        connectionUriConfig.validate();
        Assert.assertEquals("amqp://localhost:5672?transport.tcpKeepAlive=true&extra_property=false",
                connectionUriConfig.getUri());
    }

    private Map<String, Object> getBaseConfigConnection(ArrayList<Map<String, Object>> uris,
                                                        Map<String, Object> failover) {
        Map<String, Object> paramsMap = new HashMap<>();
        Map<String, Object> connectionMap = new HashMap<>();

        connectionMap.put("uris", uris);
        connectionMap.put("failover", failover);

        paramsMap.put("connection", connectionMap);
        return paramsMap;
    }

    private Map<String, Object> getConnectionUriConfig(String host, int port, String protocol,
                                                       List<String> urlOptions) {
        Map<String, Object> hostMap = new HashMap<>();
        hostMap.put("protocol", protocol);
        hostMap.put("host", host);
        hostMap.put("port", port);
        hostMap.put("urlOptions", urlOptions);
        return hostMap;
    }

    private Map<String, Object> getFailoverConfig(boolean useFailover, String jmsClientId,
                                                  List<String> failoverConfigurationOptions) {
        Map<String, Object> failoverMap = new HashMap<>();
        failoverMap.put("useFailover", useFailover);
        failoverMap.put("jmsClientId", jmsClientId);
        failoverMap.put("failoverConfigurationOptions", failoverConfigurationOptions);
        return failoverMap;
    }
}
