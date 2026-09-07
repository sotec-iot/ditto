/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import java.util.List;

import org.apache.pekko.NotUsed;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;

/**
 * Factory for creating the Pekko publish flow for Google Pub/Sub topics.
 */
@FunctionalInterface
public interface GooglePubSubPublishFlowFactory {

    /**
     * Creates a publish flow for the given topic.
     *
     * @param topic the Pub/Sub topic.
     * @param pubSubConfig the Pub/Sub configuration.
     * @return the publish flow.
     */
    Flow<PublishRequest, List<String>, NotUsed> createPublishFlow(String topic, PubSubConfig pubSubConfig);

    /**
     * Default instance that uses {@link GooglePubSub#publish(String, PubSubConfig, int)}.
     */
    static GooglePubSubPublishFlowFactory defaultFactory() {
        return (topic, config) -> GooglePubSub.publish(topic, config, 1);
    }
}
