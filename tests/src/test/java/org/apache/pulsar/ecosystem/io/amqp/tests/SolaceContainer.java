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
package org.apache.pulsar.ecosystem.io.amqp.tests;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;


/**
 * Solace container.
 */
public class SolaceContainer extends GenericContainer {

    public static final String IMAGE = "solace/solace-pubsub-standard:10.5";

    SolaceContainer(String imageName) {
        super(imageName);
//        setWaitSta();
    }

    private void setWaitSta() {
        this.waitStrategy = new HttpWaitStrategy()
                .forPort(8080)
                .forStatusCode(200)
                .withStartupTimeout(Duration.of(300, SECONDS));
    }

}
