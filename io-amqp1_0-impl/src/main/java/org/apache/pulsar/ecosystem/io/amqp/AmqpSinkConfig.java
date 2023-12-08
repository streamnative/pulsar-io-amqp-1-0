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

import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;


/**
 * QpidJms sink config.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
public class AmqpSinkConfig extends AmqpBaseConfig {

    public static AmqpSinkConfig load(Map<String, Object> config, SinkContext sinkContext) throws IOException {
        return IOConfigUtils.loadWithSecrets(config,  AmqpSinkConfig.class, sinkContext);
    }

}
