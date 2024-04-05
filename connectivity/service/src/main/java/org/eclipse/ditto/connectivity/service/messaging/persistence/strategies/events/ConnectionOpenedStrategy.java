/*
 * Copyright (c) 2019 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.connectivity.service.messaging.persistence.strategies.events;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import javax.annotation.Nullable;

import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.signals.events.ConnectionOpened;
import org.eclipse.ditto.internal.utils.persistentactors.events.EventStrategy;

/**
 * This strategy handles the {@link ConnectionOpened} event.
 */
final class ConnectionOpenedStrategy implements EventStrategy<ConnectionOpened, Connection> {

    @Override
    public Connection handle(final ConnectionOpened event, @Nullable final Connection connection,
            final long revision) {
        return checkNotNull(connection, "connection")
                .toBuilder()
                .connectionStatus(ConnectivityStatus.OPEN)
                .build();
    }
}
