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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Failover configuration.
 */
@Data
@Accessors(chain = true)
public class Failover {
    private boolean useFailover;
    private String jmsClientId;
    private List<String> failoverConfigurationOptions;

    public static Failover load(Map<String, Object> config) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(config), Failover.class);
    }
    public void validate() throws ConfigurationInvalidException {
        if (!useFailover) {
            if (failoverConfigurationOptions != null && !failoverConfigurationOptions.isEmpty()) {
                throw new ConfigurationInvalidException("failoverConfigurationOptions is only supported when failover "
                        + "is configured with useFailover set to true and jmsClientId provided");

            }
            if (StringUtils.isNotEmpty(jmsClientId)) {
                throw new ConfigurationInvalidException("jmsClientId is only supported when failover is configured "
                        + "with useFailover set to true and failoverConfigurationOptions provided");
            }
        } else {

            if (StringUtils.isEmpty(jmsClientId) && failoverConfigurationOptions != null
                    && !failoverConfigurationOptions.isEmpty()) {
                throw new ConfigurationInvalidException("failoverConfigurationOptions is only supported when failover "
                        + "is configured with useFailover set to true and jmsClientId provided");
            }

            if (StringUtils.isNotEmpty(jmsClientId)
                    && (failoverConfigurationOptions == null || failoverConfigurationOptions.isEmpty())) {
                throw new ConfigurationInvalidException("jmsClientId is only supported when failover is configured "
                        + "with useFailover set to true and failoverConfigurationOptions provided");
            }
        }

    }
    public String getFailoverPrefix(){
        if (useFailover){
            return "failover:(";
        }
        return "";
    }
    public String getFailoverSuffix(){
        if (useFailover){
            return  ")" + getFailoverConfigurationOptionsAsString();
        }
        return "";
    }

    private String getFailoverConfigurationOptionsAsString() {
        if (failoverConfigurationOptions != null && !failoverConfigurationOptions.isEmpty()) {
            return "?" + getJmsClientIdUrlOptionAsString()
                    + "&" + String.join("&", failoverConfigurationOptions);
        }
        return "";
    }

    private String getJmsClientIdUrlOptionAsString() {
        return "jms.clientID=" + jmsClientId;
    }
}
