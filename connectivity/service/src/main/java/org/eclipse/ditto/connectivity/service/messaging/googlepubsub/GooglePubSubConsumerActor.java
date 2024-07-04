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

import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.AcknowledgeRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.ReceivedMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.AcknowledgeableMessage;
import org.eclipse.ditto.connectivity.service.messaging.BaseConsumerActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.internal.RetrieveAddressStatus;
import org.eclipse.ditto.connectivity.service.messaging.monitoring.ConnectionMonitor;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.internal.utils.pekko.logging.ThreadSafeDittoLoggingAdapter;

import javax.annotation.Nullable;
import java.time.Duration;

import java.util.Map;

import static org.eclipse.ditto.connectivity.service.EnforcementFactoryFactory.newEnforcementFilterFactory;
import static org.eclipse.ditto.placeholders.PlaceholderFactory.newHeadersPlaceholder;

public class GooglePubSubConsumerActor extends BaseConsumerActor {

    static final String ACTOR_NAME_PREFIX = "googlePubSubConsumer-";

    private final ThreadSafeDittoLoggingAdapter log;


    private GooglePubSubConsumerActor(final Connection connection, final ConsumerData consumerData, final Sink<Object, NotUsed> inboundMappingSink,
                                      final ConnectivityStatusResolver connectivityStatusResolver,
                                      final ConnectivityConfig connectivityConfig) {
        super(connection, consumerData.getAddress(), inboundMappingSink, consumerData.getSource(), connectivityStatusResolver, connectivityConfig);
        this.log = DittoLoggerFactory.getThreadSafeDittoLoggingAdapter(this);
        this.setupSubscription(consumerData, getMessageMappingSink());
    }

    private void setupSubscription(final ConsumerData consumerData, final Sink<AcknowledgeableMessage, NotUsed> inboundMappingSink) {
        final var config = PubSubConfig.create();
        final var subscription = consumerData.getAddress();
        final var ackSink = GooglePubSub.acknowledge(subscription, config);
        final var subscriptionSource = GooglePubSub.subscribe(subscription, config);

        final var batchAckSink = Flow.<ReceivedMessage>create()
                .map(ReceivedMessage::ackId)
                .groupedWithin(1000, Duration.ofMinutes(1))
                .map(AcknowledgeRequest::create)
                .to(ackSink);
        final var materializer = Materializer.createMaterializer(this::getContext);

        final GooglePubSubMessageTransformer googlePubSubMessageTransformer = buildGooglePubSubMessageTransformer(consumerData, inboundMonitor,
                connectionId);

        subscriptionSource
                .alsoTo(batchAckSink)
                .map(receivedMessage -> googlePubSubMessageTransformer.transform(receivedMessage.message()))
                .map(this::getAcknowledgeableMessageForTransformationResult)
                .to(inboundMappingSink).run(materializer);
    }

    private AcknowledgeableMessage getAcknowledgeableMessageForTransformationResult(
            final TransformationResult transformationResult) {
        final var externalMessage = transformationResult.getExternalMessage().get();

        return AcknowledgeableMessage.of(externalMessage,
                () -> acknowledgeMessage(externalMessage),
                shouldRedeliver -> rejectIncomingMessage(shouldRedeliver, externalMessage));
    }

    private void acknowledgeMessage(final ExternalMessage externalMessage) {
        // TODO: No acknowleding yet.
        inboundAcknowledgedMonitor.success(externalMessage, "Sending success acknowledgement.");
    }

    private void rejectIncomingMessage(final boolean shouldRedeliver,
                                       final ExternalMessage externalMessage) {
        throw new UnsupportedOperationException("Rejection not implemented yet.");
    }


    private static boolean isExternalMessage(final TransformationResult transformationResult) {
        return transformationResult.getExternalMessage().isPresent();
    }

    @Override
    protected ThreadSafeDittoLoggingAdapter log() {
        return log;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ResourceStatus.class, this::handleAddressStatus)
                .match(RetrieveAddressStatus.class, ram -> getSender().tell(getCurrentSourceStatus(), getSelf()))
                .matchEquals(GracefulStop.START, start -> shutdown(getSender()))
                .matchEquals(GracefulStop.DONE, done -> getContext().stop(getSelf()))
                .matchAny(unhandled -> {
                    log.info("Unhandled message: {}", unhandled);
                    unhandled(unhandled);
                })
                .build();
    }


    static Props props(final Connection connection,
                       final ConsumerData consumerData,
                       final Sink<Object, NotUsed> inboundMappingSink,
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {
        return Props.create(GooglePubSubConsumerActor.class, connection, consumerData, inboundMappingSink, connectivityStatusResolver, connectivityConfig);
    }

    private void shutdown(@Nullable final ActorRef sender) {
        final var sendResponse = sender != null && !getContext().getSystem().deadLetters().equals(sender);
        final var nullableSender = sendResponse ? sender : null;
        notifyConsumerStopped(nullableSender);
    }

    private void notifyConsumerStopped(@Nullable final ActorRef sender) {
        getSelf().tell(GracefulStop.DONE, getSelf());
        if (sender != null) {
            sender.tell(Done.getInstance(), getSelf());
        }
    }

    /**
     * Message that allows gracefully stopping the consumer actor.
     */
    enum GracefulStop {
        START,
        DONE
    }


    private GooglePubSubMessageTransformer buildGooglePubSubMessageTransformer(final ConsumerData consumerData,
                                                                               final ConnectionMonitor inboundMonitor,
                                                                               final ConnectionId connectionId) {
        final Source source = consumerData.getSource();
        final String address = consumerData.getAddress();
        final Enforcement enforcement = source.getEnforcement().orElse(null);
        final EnforcementFilterFactory<Map<String, String>, Signal<?>> headerEnforcementFilterFactory =
                enforcement != null
                        ? newEnforcementFilterFactory(enforcement, newHeadersPlaceholder())
                        : input -> null;
        return new GooglePubSubMessageTransformer(connectionId, source, address, headerEnforcementFilterFactory,
                inboundMonitor);
    }

}
