/*
 * Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.connectivity.service.messaging.persistence;

import org.eclipse.ditto.internal.utils.akka.logging.DittoDiagnosticLoggingAdapter;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class UsageBasedPriorityProviderFactory extends ConnectionPriorityProviderFactory {

    /**
     * @param actorSystem the actor system in which to load the extension.
     */
    protected UsageBasedPriorityProviderFactory(final ActorSystem actorSystem) {
        super(actorSystem);
    }

    @Override
    public ConnectionPriorityProvider newProvider(final ActorRef connectionPersistenceActor,
            final DittoDiagnosticLoggingAdapter log) {

        return UsageBasedPriorityProvider.getInstance(connectionPersistenceActor, log);
    }

}
