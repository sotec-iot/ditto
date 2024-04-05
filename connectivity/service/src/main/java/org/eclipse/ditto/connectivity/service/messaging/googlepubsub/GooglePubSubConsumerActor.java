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
import org.apache.pekko.stream.connectors.googlecloud.pubsub.AcknowledgeRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.ReceivedMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ResourceStatus;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.config.GooglePubSubConsumerConfig;
import org.eclipse.ditto.connectivity.service.messaging.BaseConsumerActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.internal.RetrieveAddressStatus;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.internal.utils.pekko.logging.ThreadSafeDittoLoggingAdapter;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class GooglePubSubConsumerActor extends BaseConsumerActor {

    static final String ACTOR_NAME_PREFIX = "googlePubSubConsumer-";

    private final ThreadSafeDittoLoggingAdapter log;

    private org.apache.pekko.stream.javadsl.Source<ReceivedMessage, Cancellable> subscriptionSource;
    private Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink;
    private PubSubConfig config;


    protected GooglePubSubConsumerActor(Connection connection, String sourceAddress, Sink<Object, ?> inboundMappingSink,
                                        Source source, ConnectivityStatusResolver connectivityStatusResolver,
                                        ConnectivityConfig connectivityConfig) {
        super(connection, sourceAddress, inboundMappingSink, source, connectivityStatusResolver, connectivityConfig);
        log = DittoLoggerFactory.getThreadSafeDittoLoggingAdapter(this);
        final GooglePubSubConsumerConfig consumerConfig = connectivityConfig
                .getConnectionConfig()
                .getGooglePubSubConfig()
                .getConsumerConfig();

        final var subscription = sourceAddress;

        config = PubSubConfig.create();
        subscriptionSource = GooglePubSub.subscribe(subscription, config);

        ackSink = GooglePubSub.acknowledge(subscription, config);

        subscriptionSource
                .map(message -> {
                            System.out.println(message.toString());
                            return message.ackId();
                        })
                .groupedWithin(1000, Duration.ofMinutes(1))
                .map(AcknowledgeRequest::create)
                .to(ackSink);
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
                // TODO | check if verifyMetrics is possible
                // TODO | check if Pub/Sub has something similar like MessageRejectedException.class (kafka) that could occur
                // TODO | check if something like RestartableKafkaConsumerStream should be implemented in Google Pub/Sub
                .matchAny(unhandled -> {
                    log.info("Unhandled message: {}", unhandled);
                    unhandled(unhandled);
                })
                .build();
    }


    static Props props(final Connection connection,
                       final GooglePubSubConsumerStreamFactory streamFactory,
                       final ConsumerData consumerData,
                       final Sink<Object, NotUsed> inboundMappingSink,
                       final ConnectivityStatusResolver connectivityStatusResolver,
                       final ConnectivityConfig connectivityConfig) {
        return Props.create(GooglePubSubConsumerActor.class, connection, streamFactory, consumerData,
                inboundMappingSink, connectivityStatusResolver, connectivityConfig);
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

    static final class ReportMetrics {

        static final ReportMetrics INSTANCE = new ReportMetrics();

        private ReportMetrics() {
            // intentionally empty
        }
    }

    /**
     * Message that allows gracefully stopping the consumer actor.
     */
    enum GracefulStop {
        START,
        DONE
    }

}
