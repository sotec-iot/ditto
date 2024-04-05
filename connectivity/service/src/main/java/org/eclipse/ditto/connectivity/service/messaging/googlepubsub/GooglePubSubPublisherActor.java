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
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.apache.pekko.stream.connectors.google.GoogleSettings;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.api.OutboundSignal;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.GenericTarget;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.BasePublisherActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.SendResult;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

        GoogleSettings defaultSettings = GoogleSettings.create(context().system());
        PubSubConfig config = PubSubConfig.create();

        final var topic = connection.getTargets().get(0).getAddress();
        final var message = "Hello Google from GooglePubSubPublisherActor! ID of connection: " + connection.getId();

        PublishMessage publishMessage = PublishMessage.create(new String(Base64.getEncoder().encode(message.getBytes())));
        PublishRequest publishRequest = PublishRequest.create(Lists.newArrayList(publishMessage));

        org.apache.pekko.stream.javadsl.Source<PublishRequest, NotUsed> source =
                org.apache.pekko.stream.javadsl.Source.single(publishRequest);
        Flow<PublishRequest, List<String>, NotUsed> publishFlow = GooglePubSub.publish(topic, config, 1);

        CompletionStage<List<List<String>>> publishedMessageIds =
                source.via(publishFlow).runWith(Sink.seq(), getContext().getSystem());
        publishedMessageIds.thenAccept(messageIdLists -> {
            messageIdLists.forEach(messageIds -> {
                messageIds.forEach(messageId -> {
                    logger.info("Published message to Google Pub/Sub with ID: " + messageId);
                });
            });
        });
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
        return null;
    }

    @Override
    protected CompletionStage<SendResult> publishMessage(Signal<?> signal, @Nullable Target autoAckTarget,
                                                         GooglePubSubPublishTarget publishTarget, ExternalMessage message,
                                                         int maxTotalMessageSize, int ackSizeQuota,
                                                         @Nullable AuthorizationContext targetAuthorizationContext) {
        // TODO implement this method
        return null;
    }

    /**
     * Creates Pekko configuration object {@link org.apache.pekko.actor.Props} for this {@code BasePublisherActor}.
     *
     * @param connection                 the connection this publisher belongs to.
     * @param dryRun                     whether this publisher is only created for a test or not.
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
