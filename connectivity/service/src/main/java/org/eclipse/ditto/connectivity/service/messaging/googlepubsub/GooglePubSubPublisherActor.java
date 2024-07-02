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

    private boolean isDryRun() {
        return dryRun;
    }

    protected GooglePubSubPublisherActor(final Connection connection,
                                         boolean dryRun,
                                         final ConnectivityStatusResolver connectivityStatusResolver,
                                         final ConnectivityConfig connectivityConfig) {
        super(connection, connectivityStatusResolver, connectivityConfig);
        this.dryRun = dryRun;
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
    protected CompletionStage<SendResult> publishMessage(final Signal<?> signal,
                                                         @Nullable final Target autoAckTarget,
                                                         final GooglePubSubPublishTarget publishTarget,
                                                         final ExternalMessage message,
                                                         final int maxTotalMessageSize,
                                                         final int ackSizeQuota,
                                                         @Nullable final AuthorizationContext targetAuthorizationContext) {
        this.logger.info("Publishing message with content to GCP Pub/Sub Topic " + publishTarget.getTopic());
        final var config = PubSubConfig.create();
        final var topic = publishTarget.getTopic();
        final var publishMessage =
                PublishMessage.create(new String(Base64.getEncoder().encode(message.getTextPayload().get().getBytes())));
        final var publishRequest = PublishRequest.create(Lists.newArrayList(publishMessage));
        final var source =
                org.apache.pekko.stream.javadsl.Source.from(Collections.singletonList(publishRequest));
        final var publishFlow =
                GooglePubSub.publish(topic, config, 1);
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
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {

        return Props.create(GooglePubSubPublisherActor.class,
                connection,
                dryRun,
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
}