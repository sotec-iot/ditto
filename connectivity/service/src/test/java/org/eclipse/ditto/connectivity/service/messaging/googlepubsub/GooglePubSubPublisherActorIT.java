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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.connectivity.api.OutboundSignalFactory;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.DittoConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.AbstractPublisherActorTest;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;
import org.eclipse.ditto.internal.utils.config.DefaultScopedConfig;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test for {@link GooglePubSubPublisherActor} against a running Google Cloud Pub/Sub emulator.
 */
public class GooglePubSubPublisherActorIT extends AbstractPublisherActorTest {

    private static final String TARGET_TOPIC = "deleteme.command";
    private static final String OUTBOUND_ADDRESS = TARGET_TOPIC;
    private static final String EMULATOR_HOST = "localhost";
    private static final int EMULATOR_PORT = 8085;

    private final DittoConnectivityConfig connectivityConfig =
            DittoConnectivityConfig.of(DefaultScopedConfig.dittoScoped(CONFIG));

    @BeforeClass
    public static void checkEmulatorAvailable() {
        Assume.assumeTrue("Google Pub/Sub emulator is not available at " + EMULATOR_HOST + ":" + EMULATOR_PORT,
                isEmulatorReachable());
    }

    private static boolean isEmulatorReachable() {
        try (final Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(EMULATOR_HOST, EMULATOR_PORT), 1000);
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    protected Props getPublisherActorProps() {
        return GooglePubSubPublisherActor.props(TestConstants.createConnection(
                TestConstants.createRandomConnectionId(),
                ConnectionType.PUBSUB,
                TestConstants.Sources.SOURCES_WITH_AUTH_CONTEXT),
                false,
                mock(ConnectivityStatusResolver.class),
                connectivityConfig);
    }

    @Override
    protected void verifyPublishedMessage() {
        // Pub/Sub stream published to emulator topic
    }

    @Override
    protected void verifyPublishedMessageToReplyTarget() {
        // Pub/Sub stream published reply target to emulator topic
    }

    @Override
    protected void verifyAcknowledgements(final Supplier<Acknowledgements> ackSupplier) {
        final Acknowledgements acks = ackSupplier.get();
        assertThat(acks.getSize()).isEqualTo(1);
        final Acknowledgement ack = acks.stream().findAny().orElseThrow();
        assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.OK);
        assertThat(ack.getLabel().toString()).hasToString("please-verify");
        assertThat(ack.getEntity()).isEmpty();
    }

    @Override
    protected void publisherCreated(final TestKit kit, final ActorRef publisherActor) {
        kit.expectMsgClass(Duration.ofSeconds(10), Status.Success.class);
    }

    @Override
    protected Target decorateTarget(final Target target) {
        return target;
    }

    @Override
    protected String getOutboundAddress() {
        return OUTBOUND_ADDRESS;
    }

    @Override
    protected void setupMocks(final TestProbe probe) throws Exception {
        // no mocks needed for IT
    }

    @Override
    @Test
    public void testPublishMessage() throws Exception {
        super.testPublishMessage();
    }

    @Override
    @Test
    public void testAutoAck() throws Exception {
        setupMocks(actorSystemResource.newTestProbe());
        final var sender = actorSystemResource.newTestKit();
        final var multiMapped = OutboundSignalFactory.newMultiMappedOutboundSignal(
                List.of(
                        getMockOutboundSignalWithAutoAck("please-verify",
                                DittoHeaderDefinition.DITTO_ACKREGATOR_ADDRESS.getKey(),
                                sender.getRef().path().toSerializationFormat())
                ),
                sender.getRef()
        );

        final var publisherActor = sender.childActorOf(getPublisherActorProps());

        publisherCreated(sender, publisherActor);

        verifyAcknowledgements(() -> {
            publisherActor.tell(multiMapped, sender.getRef());
            return sender.expectMsgClass(Duration.ofSeconds(10), Acknowledgements.class);
        });
    }

    @Override
    @Test
    public void testPublishResponseToReplyTarget() throws Exception {
        super.testPublishResponseToReplyTarget();
    }
}
