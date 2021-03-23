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

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


/**
 * Amqp source config test.
 */
public class AmqpBaseConfigTest {

    @Test
    public void validateTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        AmqpBaseConfig baseConfig = AmqpBaseConfig.load(paramsMap);
        baseConfig.validate();

        Assert.assertEquals(baseConfig.getProtocol(), "amqp");
        Assert.assertEquals(baseConfig.getHost(), "localhost");
        Assert.assertEquals(baseConfig.getPort(), 5672);
        Assert.assertEquals(baseConfig.getQueue(), "test-queue");
    }

    @Test
    public void destinationConflictSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        paramsMap.put("topic", "test-topic");
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because queue and topic all set at the same time.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("The queue and topic couldn't be set at the same time.", e.getMessage());
        }
    }

    @Test
    public void destinationNotSetTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        try {
            sourceConfig.validate();
            Assert.fail("The test should fail because queue and topic all not set.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("The queue and topic all not set.", e.getMessage());
        }
    }

    @Test
    public void destinationConfigTest() throws Exception {
        Map<String, Object> paramsMap = getBaseConfig();
        paramsMap.put("queue", "test-queue");
        AmqpSourceConfig sourceConfig = AmqpSourceConfig.load(paramsMap);
        sourceConfig.validate();

        paramsMap.remove("queue");
        paramsMap.put("topic", "test-topic");
        sourceConfig = AmqpSourceConfig.load(paramsMap);
        sourceConfig.validate();
    }

    private Map<String, Object> getBaseConfig() {
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("protocol", "amqp");
        paramsMap.put("host", "localhost");
        paramsMap.put("port", 5672);
        return paramsMap;
    }

}
