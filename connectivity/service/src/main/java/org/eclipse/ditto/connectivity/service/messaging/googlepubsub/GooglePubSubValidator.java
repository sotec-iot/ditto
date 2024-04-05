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

import org.apache.pekko.actor.ActorSystem;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.validation.AbstractProtocolValidator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.function.Supplier;

/**
 * Validator for Google Pub/Sub connections.
 */
@Immutable
public class GooglePubSubValidator extends AbstractProtocolValidator {

    @Nullable
    private static GooglePubSubValidator instance;


    public static GooglePubSubValidator getInstance() {
        GooglePubSubValidator result = instance;
        if (null == result) {
            result = new GooglePubSubValidator();
            instance = result;
        }
        return result;
    }

    @Override
    public ConnectionType type() {
        return ConnectionType.PUBSUB;
    }

    @Override
    public void validate(Connection connection, DittoHeaders dittoHeaders, ActorSystem actorSystem, ConnectivityConfig connectivityConfig) {
        validateSourceConfigs(connection, dittoHeaders);
        validateTargetConfigs(connection, dittoHeaders);
        validatePayloadMappings(connection, actorSystem, connectivityConfig, dittoHeaders);
    }

    @Override
    protected void validateSource(Source source, DittoHeaders dittoHeaders, Supplier<String> sourceDescription) {
        // TODO implement this method
    }

    @Override
    protected void validateTarget(Target target, DittoHeaders dittoHeaders, Supplier<String> targetDescription) {
        // TODO implement this method
    }
}
