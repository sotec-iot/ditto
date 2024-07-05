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
import org.apache.pekko.actor.Cancellable;
import org.apache.pekko.actor.Props;
import org.apache.pekko.japi.function.Predicate;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.AcknowledgeRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.ReceivedMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.eclipse.ditto.connectivity.service.EnforcementFactoryFactory.newEnforcementFilterFactory;
import static org.eclipse.ditto.placeholders.PlaceholderFactory.newHeadersPlaceholder;

public class GooglePubSubConsumerActor extends BaseConsumerActor {

    static final String ACTOR_NAME_PREFIX = "googlePubSubConsumer-";

    private final ThreadSafeDittoLoggingAdapter log;
    private final PubSubConfig pubSubConfig;

    private final org.apache.pekko.stream.javadsl.Source<ReceivedMessage, Cancellable> subscriptionSource;

    private final Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink;

    private final Sink<TransformationResult<ReceivedMessage, ExternalMessage>, NotUsed> batchAckSink;

    private GooglePubSubConsumerActor(final Connection connection, final ConsumerData consumerData, final Sink<Object, NotUsed> inboundMappingSink,
                                      final ConnectivityStatusResolver connectivityStatusResolver,
                                      final ConnectivityConfig connectivityConfig) {
        super(connection, consumerData.getAddress(), inboundMappingSink, consumerData.getSource(), connectivityStatusResolver, connectivityConfig);
        this.log = DittoLoggerFactory.getThreadSafeDittoLoggingAdapter(this);

        this.pubSubConfig = PubSubConfig.create();

        final GooglePubSubMessageTransformer googlePubSubMessageTransformer = buildGooglePubSubMessageTransformer(consumerData, inboundMonitor,
                connectionId);

        this.subscriptionSource = GooglePubSub.subscribe(consumerData.getAddress(), this.pubSubConfig);

        this.ackSink = GooglePubSub.acknowledge(consumerData.getAddress(), this.pubSubConfig);

        this.batchAckSink = Flow.<TransformationResult<ReceivedMessage, ExternalMessage>>create()
                .map(x -> x.getTransformationInput().ackId())
                .groupedWithin(100, Duration.ofSeconds(10))
                .map(AcknowledgeRequest::create)
                .to(ackSink);

        subscriptionSource
                .map(googlePubSubMessageTransformer::transform)
                .divertTo(getTransformationFailureSink(), TransformationResult::isFailure)
                .filter(TransformationResult::isSuccess)
                .alsoTo(batchAckSink)
                .to(getTransformationSuccessSink())
                .run(Materializer.createMaterializer(this::getContext));
    }

    private <T extends TransformationResult<ReceivedMessage, ExternalMessage>> Sink<T, ?> getTransformationSuccessSink() {
        return Flow.<T>create()
                .alsoTo(Flow.<T, ExternalMessage>fromFunction(TransformationResult::getSuccessValueOrThrow)
                        .to(Sink.foreach(inboundMonitor::success)))
                .map(this::getAcknowledgeableMessageForTransformationResult)
                .to(getMessageMappingSink());
    }

    private <T extends TransformationResult<ReceivedMessage, ExternalMessage>> Sink<T, ?> getTransformationFailureSink() {
        final Predicate<GooglePubSubTransformationException> isCausedByDittoRuntimeException = exception -> {
            final var cause = exception.getCause();
            return cause instanceof DittoRuntimeException;
        };

        return Flow.<T, GooglePubSubTransformationException>fromFunction(TransformationResult::getErrorOrThrow)
                .divertTo(Flow.fromFunction(GooglePubSubConsumerActor::appendGooglePubSubAttributesToDittoRuntimeException)
                                .to(getDittoRuntimeExceptionSink()), isCausedByDittoRuntimeException)
                .to(Sink.foreach(this::recordTransformationException));
    }

    private AcknowledgeableMessage getAcknowledgeableMessageForTransformationResult(
            final TransformationResult<ReceivedMessage, ExternalMessage> transformationResult
    ) {
        final var externalMessage = transformationResult.getSuccessValueOrThrow();
        final var receivedMessage = transformationResult.getTransformationInput();

        return AcknowledgeableMessage.of(externalMessage,
                () -> new CompletableFuture<>().complete(Done.getInstance()), // ACK happened already
                shouldRedeliver -> rejectIncomingMessage(shouldRedeliver, externalMessage, receivedMessage));
    }

    private void recordTransformationException(final GooglePubSubTransformationException transformationException) {
        inboundMonitor.exception(transformationException.getCause());
    }

    private void rejectIncomingMessage(final boolean shouldRedeliver,
                                       final ExternalMessage externalMessage, final ReceivedMessage receivedMessage) {
        throw new UnsupportedOperationException("Rejection not implemented yet.");
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

    private static DittoRuntimeException appendGooglePubSubAttributesToDittoRuntimeException(
            final GooglePubSubTransformationException transformationException
    ) {
        System.out.println("Appending Google Pub/Sub attributes to DittoRuntimeException");
        final var dittoRuntimeException = (DittoRuntimeException) transformationException.getCause();
        return dittoRuntimeException.setDittoHeaders(dittoRuntimeException.getDittoHeaders()
                .toBuilder()
                .putHeaders(transformationException.getGooglePubSubAttributes())
                .build());
    }

}
