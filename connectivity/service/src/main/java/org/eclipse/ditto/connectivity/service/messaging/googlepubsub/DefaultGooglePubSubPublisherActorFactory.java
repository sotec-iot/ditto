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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.pekko.actor.Props;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;

/**
 * Default implementation, providing a {@link GooglePubSubPublisherActor}.
 */
@Immutable
public final class DefaultGooglePubSubPublisherActorFactory implements GooglePubSubPublisherActorFactory {

    @Nullable private static DefaultGooglePubSubPublisherActorFactory instance;

    private DefaultGooglePubSubPublisherActorFactory() {
        super();
    }

    /**
     * Gets an instance of the publisher actor factory.
     *
     * @return the instance.
     */
    public static DefaultGooglePubSubPublisherActorFactory getInstance() {
        DefaultGooglePubSubPublisherActorFactory result = instance;
        if (null == result) {
            result = new DefaultGooglePubSubPublisherActorFactory();
            instance = result;
        }
        return result;
    }

    @Override
    public String getActorName() {
        return GooglePubSubPublisherActor.ACTOR_NAME;
    }

    @Override
    public Props props(final Connection connection,
                       final boolean dryRun,
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {

        return GooglePubSubPublisherActor.props(connection,
                dryRun,
                connectivityStatusResolver,
                connectivityConfig);
    }

}
