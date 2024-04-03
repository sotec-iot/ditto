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
import com.typesafe.config.ConfigFactory;
import org.assertj.core.api.Assertions;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.service.config.DefaultGooglePubSubConfig;
import org.eclipse.ditto.connectivity.service.config.GooglePubSubConfig;
import org.eclipse.ditto.connectivity.service.messaging.hono.DefaultHonoConnectionFactory;
import org.eclipse.ditto.connectivity.service.messaging.hono.DefaultHonoConnectionFactoryTest;
import org.eclipse.ditto.internal.utils.pekko.ActorSystemResource;
import org.eclipse.ditto.json.JsonFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Unit test for {@link DefaultGooglePubSubConnectionFactory}.
 */
public final class DefaultGooglePubSubConnectionFactoryTest {

    private static final Config TEST_CONFIG = ConfigFactory.load("test");

    @Rule
    public final ActorSystemResource actorSystemResource = ActorSystemResource.newInstance(TEST_CONFIG);

    private GooglePubSubConfig googlePubSubConfig;

    private static Connection generateConnectionObjectFromJsonFile(final String fileName) throws IOException {
        final var testClassLoader = DefaultHonoConnectionFactoryTest.class.getClassLoader();
        try (final var connectionJsonFileStreamReader = new InputStreamReader(
                testClassLoader.getResourceAsStream(fileName)
        )) {
            return ConnectivityModelFactory.connectionFromJson(
                    JsonFactory.readFrom(connectionJsonFileStreamReader).asObject());
        }
    }

    @Before
    public void before() {
        googlePubSubConfig = new DefaultGooglePubSubConfig(actorSystemResource.getActorSystem());
    }

    @Test
    public void newInstanceWithNullActorSystemThrowsException() {
        Assertions.assertThatNullPointerException()
                .isThrownBy(() -> new DefaultHonoConnectionFactory(null, ConfigFactory.empty()))
                .withMessage("The actorSystem must not be null!")
                .withNoCause();
    }

    @Test
    public void getGooglePubSubConnection() throws IOException {
        final var userProvidedHonoConnection =
                generateConnectionObjectFromJsonFile("googlepubsub-connection-custom-test.json");
    }


}
