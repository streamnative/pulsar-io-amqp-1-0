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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;



/**
 * FailoverTest.
 */
public class FailoverTest {

    @Test
    public void validateTestConflictFailoverWithJmsClientIdAndNoUseFailover() throws Exception {

        Map<String, Object> failover = getFailoverConfig(false, "foo",
                Collections.emptyList());

        Failover failoverConfig = Failover.load(failover);

        try {
            failoverConfig.validate();
            Assert.fail("The test should fail because jmsClientId is set while useFailover in failover is false");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("jmsClientId is only supported when failover is configured with useFailover set "
                            + "to true and failoverConfigurationOptions provided",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestConflictFailoverWithFailoverConfigurationOptionsAndNoUseFailover() throws Exception {
        Map<String, Object> failover = getFailoverConfig(false, "",
                Arrays.asList("failover.maxReconnectAttempts=20"));

        Failover failoverConfig = Failover.load(failover);

        try {
            failoverConfig.validate();
            Assert.fail("The test should fail because failoverConfigurationOptions is set while useFailover "
                    + "in failover is false");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("failoverConfigurationOptions is only supported when failover is configured "
                            + "with useFailover set to true and jmsClientId provided",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestConflictFailoverWithFailoverConfigurationOptionsAndNoJmsClientId() throws Exception {
        Map<String, Object> failover = getFailoverConfig(true, "",
                Arrays.asList("failover.maxReconnectAttempts=20"));

        Failover failoverConfig = Failover.load(failover);

        try {
            failoverConfig.validate();
            Assert.fail("The test should fail because failover is configured with useFailover set to true, "
                    + "failoverConfigurationOptions are provided but no jmsClientId is provided");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("failoverConfigurationOptions is only supported when failover is configured"
                            + " with useFailover set to true and jmsClientId provided",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestConflictFailoverWithJmsClientIdButNoFailoverConfigurationOptions() throws Exception {
        Map<String, Object> failover = getFailoverConfig(true, "foo",
                Collections.emptyList());

        Failover failoverConfig = Failover.load(failover);

        try {
            failoverConfig.validate();
            Assert.fail("The test should fail because failover is configured with useFailover set to true and "
                    + "failoverConfigurationOptions but no jmsClientId is provided");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationInvalidException);
            Assert.assertEquals("jmsClientId is only supported when failover is configured with "
                            + "useFailover set to true and failoverConfigurationOptions provided",
                    e.getMessage());
        }
    }

    @Test
    public void validateTestFailoverUrlPrefix() throws Exception {
        Map<String, Object> failover = getFailoverConfig(true, "foo",
                Arrays.asList("failover.maxReconnectAttempts=20", "failover.useReconnectBackOff=true"));

        Failover failoverConfig = Failover.load(failover);
        failoverConfig.validate();
        Assert.assertEquals("failover:("
                , failoverConfig.getFailoverPrefix());
    }

    @Test
    public void validateTestFailoverUrlSuffix() throws Exception {
        Map<String, Object> failover = getFailoverConfig(true, "",
                Collections.emptyList());

        Failover failoverConfig = Failover.load(failover);
        failoverConfig.validate();
        Assert.assertEquals(")", failoverConfig.getFailoverSuffix());
    }

    @Test
    public void validateTestFailoverUrlSuffixWithFailoverOptions() throws Exception {
        Map<String, Object> failover = getFailoverConfig(true, "foo",
                Arrays.asList("failover.maxReconnectAttempts=20", "failover.useReconnectBackOff=true"));

        Failover failoverConfig = Failover.load(failover);
        failoverConfig.validate();
        Assert.assertEquals(")?jms.clientID=foo&failover.maxReconnectAttempts=20"
                + "&failover.useReconnectBackOff=true", failoverConfig.getFailoverSuffix());
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
