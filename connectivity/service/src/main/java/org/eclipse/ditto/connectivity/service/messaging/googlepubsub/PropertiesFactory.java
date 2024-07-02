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

import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.eclipse.ditto.connectivity.model.Connection;

/**
 * Creates PubSub properties from a given {@link org.eclipse.ditto.connectivity.model.Connection} configuration.
 */
final class PropertiesFactory {

    private final Connection connection;
    private final PubSubConfig config;
    private final String projectId;

    PropertiesFactory(Connection connection, PubSubConfig config, String projectId) {
        this.connection = connection;
        this.config = config;
        this.projectId = projectId;
    }


    /**
     * Returns an instance of the factory.
     *
     * @param connection the Kafka connection.
     * @param config     the Kafka configuration settings.
     * @param clientId   the client ID.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    static PropertiesFactory newInstance(final Connection connection, final PubSubConfig config, final String clientId) {
        return new PropertiesFactory(connection, config, clientId);
    }

}
