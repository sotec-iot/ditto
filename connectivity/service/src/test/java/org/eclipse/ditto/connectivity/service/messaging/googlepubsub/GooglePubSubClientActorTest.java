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


import com.typesafe.config.ConfigFactory;
import org.apache.pekko.Done;
import org.apache.pekko.actor.*;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.api.BaseClientState;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.model.signals.commands.modify.CloseConnection;
import org.eclipse.ditto.connectivity.model.signals.commands.modify.OpenConnection;
import org.eclipse.ditto.connectivity.service.messaging.AbstractBaseClientActorTest;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;

import static java.util.Collections.singletonList;
import static org.eclipse.ditto.connectivity.service.messaging.TestConstants.Authorization.AUTHORIZATION_CONTEXT;

@RunWith(MockitoJUnitRunner.class)
public final class GooglePubSubClientActorTest extends AbstractBaseClientActorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GooglePubSubClientActorTest.class);
    private static final Status.Success CONNECTED_SUCCESS = new Status.Success(BaseClientState.CONNECTED);
    private static final Status.Success DISCONNECTED_SUCCESS = new Status.Success(BaseClientState.DISCONNECTED);
    private static ActorSystem actorSystem;
    private static ServerSocket mockServer;
    private ConnectionId connectionId;
    private Connection connection;
    private static final String SUBSCRIPTION = "pubsubtest.event";

    private static final Target TARGET = ConnectivityModelFactory.newTargetBuilder()
            .address(SUBSCRIPTION)
            .authorizationContext(AUTHORIZATION_CONTEXT)
            .qos(0)
            .topics(Topic.TWIN_EVENTS)
            .build();


    @BeforeClass
    public static void setUp() {
        actorSystem = ActorSystem.create("PekkoTestSystem", TestConstants.CONFIG);
    }

    private static void startMockServer() {
        mockServer = TestConstants.newMockServer();
        LOGGER.info("Started mock server on port {}", mockServer.getLocalPort());
    }

    @AfterClass
    public static void tearDown() {
        actorSystem.terminate();
        stopMockServer();
    }

    private static void stopMockServer() {
        if (null != mockServer) {
            try {
                mockServer.close();
                LOGGER.info("Successfully closed mock server.");
            } catch (final IOException e) {
                LOGGER.info("Got unexpected exception while closing the mock server.", e);
            }
        } else {
            LOGGER.info("Could not stop mock server as it unexpectedly was <null>.");
        }
    }

    @Before
    public void initializeConnection() {
        startMockServer(); // Start the mock server before each test
        connectionId = TestConstants.createRandomConnectionId();
        connection = ConnectivityModelFactory.newConnectionBuilder(connectionId, ConnectionType.PUBSUB,
                        ConnectivityStatus.CLOSED, "")
                .targets(singletonList(TARGET))
                .failoverEnabled(true)
                .build();
        System.out.println(connection.getConnectionType());
    }

    @Test
    public void connectAndDisconnect() {
        new TestKit(actorSystem) {{
            final TestProbe probe = new TestProbe(getSystem());
            final Props props = getGooglePubSubClientActorProps(probe.ref(), connection);
            final ActorRef googlePubSubClientActor = actorSystem.actorOf(props);

            googlePubSubClientActor.tell(OpenConnection.of(connection.getId(), DittoHeaders.empty()), getRef());
            expectMsg(Duration.ofSeconds(50), CONNECTED_SUCCESS);

            googlePubSubClientActor.tell(CloseConnection.of(connection.getId(), DittoHeaders.empty()), getRef());
            expectMsg(DISCONNECTED_SUCCESS);

        }};
    }

    private Props getGooglePubSubClientActorProps(final ActorRef ref, final Connection connection) {
        return getGooglePubSubClientActorProps(ref, new Status.Success(Done.done()), connection);
    }

    private Props getGooglePubSubClientActorProps(final ActorRef ref, final Status.Status status,
                                                  final Connection connection) {
        return GooglePubSubClientActor.propsForTests(connection, ref, ref, dittoHeaders);
    }


    @Override
    protected Connection getConnection(boolean isSecure) {
        return connection;
    }

    @Override
    protected Props createClientActor(final ActorRef proxyActor, final Connection connection) {
        return GooglePubSubClientActor.props(connection, proxyActor, proxyActor, dittoHeaders, ConfigFactory.empty());
    }

    @Override
    protected ActorSystem getActorSystem() {
        return null;
    }
}