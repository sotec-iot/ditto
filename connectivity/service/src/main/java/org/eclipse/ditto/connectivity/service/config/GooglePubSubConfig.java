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
package org.eclipse.ditto.connectivity.service.config;

import javax.annotation.concurrent.Immutable;

/**
 * Provides configuration settings for the Google Pub/Sub connection.
 */
@Immutable
public interface GooglePubSubConfig {

    /**
     * Returns the configuration for Google PubSub consumer.
     *
     * @return the configuration.
     */
    GooglePubSubConsumerConfig getConsumerConfig();

    /**
     * Returns the configuration for Google PubSub producer.
     *
     * @return the configuration.
     */
    GooglePubSubProducerConfig getProducerConfig();
}
