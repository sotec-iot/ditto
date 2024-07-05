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
import org.apache.pekko.stream.javadsl.Source;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.*;
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

/**
 * Google Pub/Sub consumer actor for handling incoming messages.
 */
public class GooglePubSubConsumerActor extends BaseConsumerActor {

    static final String ACTOR_NAME_PREFIX = "googlePubSubConsumer-";

    private final ThreadSafeDittoLoggingAdapter log;

    private final PubSubConfig pubSubConfig;
    private final String subscription;

    private GooglePubSubConsumerActor(final Connection connection,
                                      final ConsumerData consumerData,
                                      final Sink<Object, NotUsed> inboundMappingSink,
                                      final ConnectivityStatusResolver connectivityStatusResolver,
                                      final ConnectivityConfig connectivityConfig) {
        super(connection, consumerData.getAddress(), inboundMappingSink, consumerData.getSource(), connectivityStatusResolver, connectivityConfig);
        this.log = DittoLoggerFactory.getThreadSafeDittoLoggingAdapter(this);
        this.pubSubConfig = PubSubConfig.create();
        this.subscription = consumerData.getAddress();
        this.setUpSubscription(consumerData);
    }

    /**
     * Sets up the subscription to Google Pub/Sub.
     *
     * @param consumerData The consumer data containing necessary details.
     */
    private void setUpSubscription(final ConsumerData consumerData) {
        final var googlePubSubMessageTransformer = buildGooglePubSubMessageTransformer(consumerData, inboundMonitor, connectionId);
        this.getSubscriptionSource()
                .map(googlePubSubMessageTransformer::transform)
                .divertTo(this.getTransformationFailureSink(), TransformationResult::isFailure)
                .alsoTo(this.getBatchAckSink())
                .to(this.getTransformationSuccessSink())
                .run(Materializer.createMaterializer(this::getContext));
    }

    /**
     * Returns the subscription source for Google Pub/Sub.
     *
     * @return Source of received messages.
     */
    private Source<ReceivedMessage, Cancellable> getSubscriptionSource() {
        return GooglePubSub.subscribe(this.subscription, this.pubSubConfig);
    }

    /**
     * Returns the acknowledgement sink for Google Pub/Sub.
     *
     * @return Sink for acknowledging messages.
     */
    private Sink<AcknowledgeRequest, CompletionStage<Done>> getAckSink() {
        return GooglePubSub.acknowledge(this.subscription, this.pubSubConfig);
    }

    /**
     * Returns the batch acknowledgement sink for Google Pub/Sub.
     *
     * @return Sink for batch acknowledging messages.
     */
    private Sink<TransformationResult<ReceivedMessage, ExternalMessage>, NotUsed> getBatchAckSink() {
        return Flow.<TransformationResult<ReceivedMessage, ExternalMessage>>create()
                .map(this::extractAckId)
                .groupedWithin(100, Duration.ofSeconds(10))
                .map(AcknowledgeRequest::create)
                .to(this.getAckSink());
    }

    /**
     * Extracts the acknowledgment ID from a transformation result.
     *
     * @param result The transformation result.
     * @return Acknowledgment ID string.
     */
    private String extractAckId(TransformationResult<ReceivedMessage, ExternalMessage> result) {
        return result.getTransformationInput().ackId();
    }

    /**
     * Returns the sink for successful message transformations.
     *
     * @param <T> Type of transformation result.
     * @return Sink for successful transformations.
     */
    private <T extends TransformationResult<ReceivedMessage, ExternalMessage>> Sink<T, ?> getTransformationSuccessSink() {
        return Flow.<T>create()
                .alsoTo(Flow.<T, ExternalMessage>fromFunction(TransformationResult::getSuccessValueOrThrow)
                        .to(Sink.foreach(inboundMonitor::success)))
                .map(this::getAcknowledgeableMessageForTransformationResult)
                .to(getMessageMappingSink());
    }

    /**
     * Returns the sink for handling transformation failures.
     *
     * @param <T> Type of transformation result.
     * @return Sink for transformation failures.
     */
    private <T extends TransformationResult<ReceivedMessage, ExternalMessage>> Sink<T, ?> getTransformationFailureSink() {
        final Predicate<GooglePubSubTransformationException> isCausedByDittoRuntimeException = exception -> exception.getCause() instanceof DittoRuntimeException;

        return Flow.<T, GooglePubSubTransformationException>fromFunction(TransformationResult::getErrorOrThrow)
                .divertTo(Flow.fromFunction(this::handleGooglePubSubException)
                        .to(getDittoRuntimeExceptionSink()), isCausedByDittoRuntimeException)
                .to(Sink.foreach(this::recordTransformationException));
    }

    /**
     * Handles Google Pub/Sub exception by appending attributes to DittoRuntimeException.
     *
     * @param transformationException The transformation exception.
     * @return DittoRuntimeException with appended attributes.
     */
    private DittoRuntimeException handleGooglePubSubException(final GooglePubSubTransformationException transformationException) {
        log.info("Appending Google Pub/Sub attributes to DittoRuntimeException");
        final var dittoRuntimeException = (DittoRuntimeException) transformationException.getCause();
        return dittoRuntimeException.setDittoHeaders(dittoRuntimeException.getDittoHeaders()
                .toBuilder()
                .putHeaders(transformationException.getGooglePubSubAttributes())
                .build());
    }

    /**
     * Creates an AcknowledgeableMessage from a transformation result.
     *
     * @param transformationResult The transformation result.
     * @return AcknowledgeableMessage instance.
     */
    private AcknowledgeableMessage getAcknowledgeableMessageForTransformationResult(
            final TransformationResult<ReceivedMessage, ExternalMessage> transformationResult) {
        final var externalMessage = transformationResult.getSuccessValueOrThrow();
        return AcknowledgeableMessage.of(externalMessage,
                // Google Pub/Sub ACK occurs in a subsequent step regardless of whether Ditto acknowledges the message.
                () -> CompletableFuture.completedFuture(Done.getInstance()),
                shouldRedeliver -> CompletableFuture.completedFuture(Done.getInstance()));
    }

    /**
     * Records a transformation exception.
     *
     * @param transformationException The transformation exception to record.
     */
    private void recordTransformationException(final GooglePubSubTransformationException transformationException) {
        inboundMonitor.exception(transformationException.getCause());
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

    /**
     * Initiates shutdown of the actor.
     *
     * @param sender The sender actor reference.
     */
    private void shutdown(@Nullable final ActorRef sender) {
        final var sendResponse = sender != null && !getContext().getSystem().deadLetters().equals(sender);
        final var nullableSender = sendResponse ? sender : null;
        notifyConsumerStopped(nullableSender);
    }

    /**
     * Notifies that the consumer has stopped.
     *
     * @param sender The sender actor reference.
     */
    private void notifyConsumerStopped(@Nullable final ActorRef sender) {
        getSelf().tell(GracefulStop.DONE, getSelf());
        if (sender != null) {
            sender.tell(Done.getInstance(), getSelf());
        }
    }

    /**
     * Props for creating instances of this actor.
     *
     * @param connection                 The connection details.
     * @param consumerData               The consumer data.
     * @param inboundMappingSink         The sink for inbound mapping.
     * @param connectivityStatusResolver The connectivity status resolver.
     * @param connectivityConfig         The connectivity configuration.
     * @return Props instance for creating this actor.
     */
    static Props props(final Connection connection,
                       final ConsumerData consumerData,
                       final Sink<Object, NotUsed> inboundMappingSink,
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {
        return Props.create(GooglePubSubConsumerActor.class, connection, consumerData, inboundMappingSink, connectivityStatusResolver, connectivityConfig);
    }

    /**
     * Builds the message transformer for Google Pub/Sub.
     *
     * @param consumerData The consumer data.
     * @return GooglePubSubMessageTransformer instance.
     */
    private GooglePubSubMessageTransformer buildGooglePubSubMessageTransformer(final ConsumerData consumerData,
                                                                               final ConnectionMonitor inboundMonitor,
                                                                               final ConnectionId connectionId) {
        final org.eclipse.ditto.connectivity.model.Source source = consumerData.getSource();
        final Enforcement enforcement = source.getEnforcement().orElse(null);
        final EnforcementFilterFactory<Map<String, String>, Signal<?>> headerEnforcementFilterFactory =
                enforcement != null
                        ? newEnforcementFilterFactory(enforcement, newHeadersPlaceholder())
                        : input -> null;
        return new GooglePubSubMessageTransformer(connectionId, source, this.subscription, headerEnforcementFilterFactory,
                inboundMonitor);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        shutdown(null);
    }

    /**
     * Handles gracefully stopping the consumer actor.
     */
    private void shutdown() {
        getContext().stop(getSelf());
    }

    /**
     * Message that allows gracefully stopping the consumer actor.
     */
    enum GracefulStop {
        START,
        DONE
    }

}