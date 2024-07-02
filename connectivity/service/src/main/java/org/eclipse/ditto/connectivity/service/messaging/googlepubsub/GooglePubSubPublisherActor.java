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

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.entity.id.EntityId;
import org.eclipse.ditto.base.model.entity.id.WithEntityId;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.api.OutboundSignal;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.GenericTarget;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.BasePublisherActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.SendResult;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class GooglePubSubPublisherActor extends BasePublisherActor<GooglePubSubPublishTarget> {

    /**
     * The name of this Actor in the ActorSystem.
     */
    static final String ACTOR_NAME = "googlePubSubPublisherActor";

    private final boolean dryRun;
    private final PubSubConfig pubSubConfig;

    private boolean isDryRun() {
        return dryRun;
    }

    protected GooglePubSubPublisherActor(final Connection connection,
                                         boolean dryRun,
                                         final PubSubConfig pubSubConfig,
                                         final ConnectivityStatusResolver connectivityStatusResolver,
                                         final ConnectivityConfig connectivityConfig) {
        super(connection, connectivityStatusResolver, connectivityConfig);
        this.dryRun = dryRun;
        this.pubSubConfig = pubSubConfig;
    }


    @Override
    protected void preEnhancement(ReceiveBuilder receiveBuilder) {
        receiveBuilder
                .match(OutboundSignal.Mapped.class, this::isDryRun, outbound ->
                        logger.withCorrelationId(outbound.getSource())
                                .info("Message dropped in dry run mode: {}", outbound))
                .matchEquals(GracefulStop.INSTANCE, unused -> this.stopGracefully());
    }

    @Override
    protected void postEnhancement(ReceiveBuilder receiveBuilder) {
        // noop
    }

    @Override
    protected GooglePubSubPublishTarget toPublishTarget(GenericTarget target) {
        // TODO implement this method
        // This method transforms a GenericTarget to a GooglePubSubPublishTarget
        return GooglePubSubPublishTarget.fromTargetAddress(target.getAddress());
    }

    @Override
    protected CompletionStage<SendResult> publishMessage(Signal<?> signal, @Nullable Target autoAckTarget,
                                                         GooglePubSubPublishTarget publishTarget,
                                                         ExternalMessage message, int maxTotalMessageSize,
                                                         int ackSizeQuota,
                                                         @Nullable AuthorizationContext targetAuthorizationContext) {
        final String topic = publishTarget.getTopic();
        System.out.println("Publishing message with content to GCP Pub/Sub Topic " + topic);

        PublishMessage publishMessage =
                PublishMessage.create(new String(Base64.getEncoder().encode(message.getTextPayload().get().getBytes())));

        PublishRequest publishRequest = PublishRequest.create(Lists.newArrayList(publishMessage));

        org.apache.pekko.stream.javadsl.Source<PublishRequest, NotUsed> source =
                org.apache.pekko.stream.javadsl.Source.from(Collections.singletonList(publishRequest));

        Flow<PublishRequest, List<String>, NotUsed> publishFlow =
                GooglePubSub.publish(topic, pubSubConfig, 1);

        return source.via(publishFlow)
                .runWith(Sink.seq(), this.getContext().getSystem())
                .toCompletableFuture()
                .thenApply(publishedMessageIds -> new SendResult(null, null, null));
    }

    /**
     * Creates Pekko configuration object {@link org.apache.pekko.actor.Props} for this {@code BasePublisherActor}.
     *
     * @param connection                 the connection this publisher belongs to.
     * @param dryRun                     whether this publisher is only created for a test or not.
     * @param pubSubConfig               the configuration for Google Pub/Sub.
     * @param connectivityStatusResolver connectivity status resolver to resolve occurred exceptions to a connectivity
     *                                   status.
     * @param connectivityConfig         the config of the connectivity service with potential overwrites.
     * @return the Pekko configuration Props object.
     */
    static Props props(final Connection connection,
                       final boolean dryRun,
                       final PubSubConfig pubSubConfig,
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {

        return Props.create(GooglePubSubPublisherActor.class,
                connection,
                dryRun,
                pubSubConfig,
                connectivityStatusResolver,
                connectivityConfig);
    }


    @Override
    public void preStart() throws Exception {
        super.preStart();
        reportInitialConnectionState();
    }

    private void reportInitialConnectionState() {
        logger.info("Publisher ready.");
        getContext().getParent().tell(new Status.Success(Done.done()), getSelf());
    }

    private void stopGracefully() {
        logger.debug("Stopping myself.");
        getContext().stop(getSelf());
    }


    /**
     * Message that allows gracefully stopping the publisher actor.
     */
    static final class GracefulStop {

        static final GracefulStop INSTANCE = new GooglePubSubPublisherActor.GracefulStop();

        private GracefulStop() {
            // intentionally empty
        }

    }

    private static final class ProducerCallback implements Function<RecordMetadata, SendResult> {

        private final Signal<?> signal;
        private final int ackSizeQuota;
        private int currentQuota;
        private final Connection connection;
        @Nullable private final AcknowledgementLabel autoAckLabel;

        private ProducerCallback(final Signal<?> signal,
                                 @Nullable final AcknowledgementLabel autoAckLabel,
                                 final int ackSizeQuota,
                                 final Connection connection) {

            this.signal = signal;
            this.autoAckLabel = autoAckLabel;
            this.ackSizeQuota = ackSizeQuota;
            this.connection = connection;
        }

        @Override
        public SendResult apply(final RecordMetadata recordMetadata) {
            return buildResponseFromMetadata(recordMetadata);
        }


        private SendResult buildResponseFromMetadata(@Nullable final RecordMetadata metadata) {
            final DittoHeaders dittoHeaders = signal.getDittoHeaders();
            final boolean verbose = isDebugEnabled() && metadata != null;
            final JsonObject ackPayload = verbose ? toPayload(metadata) : null;
            final HttpStatus httpStatus = verbose ? HttpStatus.OK : HttpStatus.NO_CONTENT;
            final Optional<EntityId> entityIdOptional =
                    WithEntityId.getEntityIdOfType(EntityId.class, signal);
            @Nullable final Acknowledgement issuedAck;
            if (entityIdOptional.isPresent() && null != autoAckLabel) {
                issuedAck = Acknowledgement.of(autoAckLabel,
                        entityIdOptional.get(),
                        httpStatus,
                        dittoHeaders,
                        ackPayload);
            } else {
                issuedAck = null;
            }

            return new SendResult(issuedAck, dittoHeaders);
        }

        private JsonObject toPayload(final RecordMetadata metadata) {
            final JsonObjectBuilder builder = JsonObject.newBuilder();
            currentQuota = ackSizeQuota;
            if (metadata.hasTimestamp()) {
                builder.set("timestamp", metadata.timestamp(), this::isQuotaSufficient);
            }
            builder.set("serializedKeySize", metadata.serializedKeySize(), this::isQuotaSufficient);
            builder.set("serializedValueSize", metadata.serializedValueSize(), this::isQuotaSufficient);
            builder.set("topic", metadata.topic(), this::isQuotaSufficient);
            builder.set("partition", metadata.partition(), this::isQuotaSufficient);
            if (metadata.hasOffset()) {
                builder.set("offset", metadata.offset(), this::isQuotaSufficient);
            }

            return builder.build();
        }

        private boolean isQuotaSufficient(final JsonField field) {
            final int fieldSize = field.getKey().length() +
                    (field.getValue().isString() ? field.getValue().asString().length() : 8);
            if (fieldSize <= currentQuota) {
                currentQuota -= fieldSize;
                return true;
            } else {
                return false;
            }
        }

        private boolean isDebugEnabled() {
            final Map<String, String> specificConfig = connection.getSpecificConfig();

            return Boolean.parseBoolean(specificConfig.getOrDefault("debugEnabled", Boolean.FALSE.toString()));
        }

    }


}