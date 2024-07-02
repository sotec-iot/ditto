/*
 * Copyright (c) 2021 Contributors to the Eclipse Foundation
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
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;
import static org.eclipse.ditto.base.model.common.HttpStatus.SERVICE_UNAVAILABLE;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.common.ByteBufferUtils;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.api.ExternalMessageFactory;
import org.eclipse.ditto.connectivity.api.OutboundSignal;
import org.eclipse.ditto.connectivity.api.OutboundSignalFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.MessageSendingFailedException;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.service.config.DittoConnectivityConfig;
import org.eclipse.ditto.connectivity.service.config.KafkaProducerConfig;
import org.eclipse.ditto.connectivity.service.messaging.AbstractPublisherActorTest;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;
import org.eclipse.ditto.internal.utils.config.DefaultScopedConfig;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.things.model.signals.events.ThingDeleted;
import org.eclipse.ditto.things.model.signals.events.ThingEvent;
import org.junit.Test;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.testkit.javadsl.TestKit;
import scala.concurrent.duration.FiniteDuration;

/**
 * Unit test for {@link GooglePubSubPublisherActor}.
 */
public class GooglePubSubPublisherActorTest extends AbstractPublisherActorTest {

    private static final String TARGET_TOPIC = "deleteme.command";
    private static final String OUTBOUND_ADDRESS = TARGET_TOPIC;

    private final Queue<ProducerRecord<String, ByteBuffer>> published = new ConcurrentLinkedQueue<>();

    private final DittoConnectivityConfig connectivityConfig =
            DittoConnectivityConfig.of(DefaultScopedConfig.dittoScoped(CONFIG));
    private final KafkaProducerConfig kafkaConfig = connectivityConfig
            .getConnectionConfig()
            .getKafkaConfig()
            .getProducerConfig();



    @Override
    protected Props getPublisherActorProps() {
        return GooglePubSubPublisherActor.props(TestConstants.createConnection(),
                false,
                mock(ConnectivityStatusResolver.class),
                connectivityConfig);
    }

    @Override
    protected void verifyPublishedMessage() {
        Awaitility.await("wait for published messages").until(() -> !published.isEmpty());
        final ProducerRecord<String, ByteBuffer> record = checkNotNull(published.poll());
        assertThat(published).isEmpty();
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo(TARGET_TOPIC);
        assertThat(record.key()).isEqualTo("keyA");
        assertThat(record.value()).isEqualTo(ByteBufferUtils.fromUtf8String("payload"));
        final List<Header> headers = Arrays.asList(record.headers().toArray());
        shouldContainHeader(headers, "thing_id", TestConstants.Things.THING_ID.toString());
        shouldContainHeader(headers, "suffixed_thing_id", TestConstants.Things.THING_ID + ".some.suffix");
        shouldContainHeader(headers, "prefixed_thing_id", "some.prefix." + TestConstants.Things.THING_ID);
        shouldContainHeader(headers, "eclipse", "ditto");
        shouldContainHeader(headers, "device_id", TestConstants.Things.THING_ID.toString());
        shouldContainHeader(headers, "ditto-connection-id");
        final Optional<Header> expectedHeader = headers.stream()
                .filter(header -> header.key().equals("ditto-connection-id"))
                .findAny();
        assertThat(expectedHeader).isPresent();
        assertThat(new String(expectedHeader.get().value()))
                .isNotEqualTo("hallo");//verify that header mapping has no effect
    }

    @Override
    protected void verifyPublishedMessageToReplyTarget() {
        Awaitility.await().until(() -> !published.isEmpty());
        final ProducerRecord<String, ByteBuffer> record = checkNotNull(published.poll());
        assertThat(published).isEmpty();
        assertThat(record.topic()).isEqualTo("replyTarget");
        assertThat(record.key()).isEqualTo("thing:id");
        final List<Header> headers = Arrays.asList(record.headers().toArray());
        shouldContainHeader(headers, "correlation-id", TestConstants.CORRELATION_ID);
        shouldContainHeader(headers, "mappedHeader2", "thing:id");
    }

    @Override
    protected void verifyAcknowledgements(final Supplier<Acknowledgements> ackSupplier) {
        final Acknowledgements acks = ackSupplier.get();
        assertThat(acks.getSize()).isEqualTo(1);
        final Acknowledgement ack = acks.stream().findAny().orElseThrow();
        assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);
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
    protected void setupMocks(TestProbe probe) throws Exception {

    }

    private void shouldContainHeader(final List<Header> headers, final String key, final String value) {
        final RecordHeader expectedHeader = new RecordHeader(key, value.getBytes(StandardCharsets.US_ASCII));
        assertThat(headers).contains(expectedHeader);
    }

    private void shouldContainHeader(final List<Header> headers, final String key) {
        final Optional<Header> expectedHeader = headers.stream().filter(header -> header.key().equals(key)).findAny();
        assertThat(expectedHeader).isPresent();
    }

}
