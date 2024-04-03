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

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;

/**
 * Default implementation of {@link GooglePubSubConnectionFactory}.
 */
public final class DefaultGooglePubSubConnectionFactory extends GooglePubSubConnectionFactory {
    
    public DefaultGooglePubSubConnectionFactory(final ActorSystem actorSystem, final Config config) {
        super();
    }

}
