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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.japi.pf.FSMStateFunctionBuilder;
import org.apache.pekko.pattern.Patterns;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.api.BaseClientState;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.model.signals.commands.modify.TestConnection;
import org.eclipse.ditto.connectivity.service.config.ConnectionThrottlingConfig;
import org.eclipse.ditto.connectivity.service.config.GooglePubSubConfig;
import org.eclipse.ditto.connectivity.service.messaging.BaseClientActor;
import org.eclipse.ditto.connectivity.service.messaging.BaseClientData;
import org.eclipse.ditto.connectivity.service.messaging.internal.ClientConnected;
import org.eclipse.ditto.connectivity.service.messaging.internal.ClientDisconnected;
import org.eclipse.ditto.connectivity.service.messaging.internal.ConnectionFailure;
import org.eclipse.ditto.connectivity.service.util.ConnectivityMdcEntryKey;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GooglePubSubClientActor extends BaseClientActor {

    private final Set<ActorRef> pendingStatusReportsFromStreams;
    private final PropertiesFactory propertiesFactory;
    private CompletableFuture<Status.Status> testConnectionFuture = null;
    private final List<ActorRef> googlePubSubConsumerActors;
    private final GooglePubSubConfig googlePubSubConfig;

    private GooglePubSubClientActor(final Connection connection,
                                    final ActorRef commandForwarderActor,
                                    final ActorRef connectionActor,
                                    // TODO check if needed
                                    final GooglePubSubPublisherActorFactory publisherActorFactory,
                                    final DittoHeaders dittoHeaders,
                                    final Config connectivityConfigOverwrites) {

        super(connection, commandForwarderActor, connectionActor, dittoHeaders, connectivityConfigOverwrites);
        googlePubSubConfig = connectivityConfig().getConnectionConfig().getGooglePubSubConfig();
        googlePubSubConsumerActors = new ArrayList<>();
        this.propertiesFactory = PropertiesFactory.newInstance(connection, googlePubSubConfig, getClientId(connection.getId()));
        pendingStatusReportsFromStreams = new HashSet<>();
    }

    /**
     * Creates Pekko configuration object for this actor.
     *
     * @param connection                   the connection.
     * @param commandForwarderActor        the actor used to send signals into the ditto cluster.
     * @param connectionActor              the connectionPersistenceActor which created this client.
     * @param dittoHeaders                 headers of the command that caused this actor to be created.
     * @param connectivityConfigOverwrites the overwrites for the connectivity config for the given connection.
     * @return the Pekko configuration Props object.
     */
    public static Props props(final Connection connection,
                              final ActorRef commandForwarderActor,
                              final ActorRef connectionActor,
                              final DittoHeaders dittoHeaders,
                              final Config connectivityConfigOverwrites) {

        return Props.create(GooglePubSubClientActor.class, validateConnection(connection), commandForwarderActor,
                connectionActor, dittoHeaders, connectivityConfigOverwrites);
    }

    static Props propsForTests(final Connection connection,
                               final ActorRef proxyActor,
                               final ActorRef connectionActor,
                               final DittoHeaders dittoHeaders) {

        return Props.create(GooglePubSubClientActor.class, validateConnection(connection), proxyActor, connectionActor, dittoHeaders, ConfigFactory.empty());
    }

    private static Connection validateConnection(final Connection connection) {
        // nothing to do so far
        return connection;
    }

    @Override
    protected FSMStateFunctionBuilder<BaseClientState, BaseClientData> inTestingState() {
        return super.inTestingState()
                .event(Status.Status.class, (e, d) -> !Objects.equals(getSender(), getSelf()),
                        (status, data) -> handleStatusReportFromChildren(status))
                .event(ClientConnected.class, BaseClientData.class, (event, data) -> {
                    final String url = data.getConnection().getUri();
                    final String message = "Kafka connection to " + url + " established successfully";
                    completeTestConnectionFuture(new Status.Success(message));
                    return stay();
                })
                .event(ConnectionFailure.class, BaseClientData.class, (event, data) -> {
                    completeTestConnectionFuture(new Status.Failure(event.getFailure().cause()));
                    return stay();
                });
    }

    @Override
    protected FSMStateFunctionBuilder<BaseClientState, BaseClientData> inConnectingState() {
        return super.inConnectingState()
                .event(Status.Status.class, (status, data) -> handleStatusReportFromChildren(status));
    }

    @Override
    protected CompletionStage<Status.Status> doTestConnection(final TestConnection testConnectionCommand) {
        if (testConnectionFuture != null) {
            final Exception error =
                    new IllegalStateException("Can't test new connection since a test is already running.");
            return CompletableFuture.completedFuture(new Status.Failure(error));
        }
        testConnectionFuture = new CompletableFuture<>();
        final DittoHeaders dittoHeaders = testConnectionCommand.getDittoHeaders();
        final String correlationId = dittoHeaders.getCorrelationId().orElse(null);
        connectClient(true, testConnectionCommand.getEntityId(), correlationId);
        return testConnectionFuture;
    }

    @Override
    protected CompletionStage<Void> stopConsuming() {
        final var timeout = Duration.ofMinutes(2L);
        final CompletableFuture<?>[] futures = googlePubSubConsumerActors.stream()
                .map(consumer -> Patterns.ask(consumer, GooglePubSubConsumerActor.GracefulStop.START, timeout))
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    @Override
    protected void doConnectClient(final Connection connection, @Nullable final ActorRef origin) {
        connectClient(false, connection.getId(), null);
    }

    @Override
    protected void doDisconnectClient(final Connection connection, @Nullable final ActorRef origin,
                                      final boolean shutdownAfterDisconnect) {
        getSelf().tell(ClientDisconnected.of(origin, shutdownAfterDisconnect), origin);
    }

    @Override
    protected ActorRef getPublisherActor() {
        return null;
    }

    /**
     * Start Google PubSub publisher and consumers, expect "Status.Success" from each of them, then send "ClientConnected" to self.
     *
     * @param dryRun        if set to true, exchange no message between the broker and the Ditto cluster.
     * @param connectionId  the ID of the connection to connect the client for.
     * @param correlationId the correlation ID for logging or {@code null} if no correlation ID is known.
     */
    private void connectClient(final boolean dryRun, final ConnectionId connectionId,
                               @Nullable final CharSequence correlationId) {

        // start publisher
        startKafkaPublisher(dryRun, connectionId, correlationId);
        // start consumers
        startGooglePubSubConsumers(dryRun, connectionId, correlationId);
    }

    private void startKafkaPublisher(final boolean dryRun, final ConnectionId connectionId,
                                     @Nullable final CharSequence correlationId) {
        // TODO implement
    }

    private void startGooglePubSubConsumers(final boolean dryRun, final ConnectionId connectionId,
                                            @Nullable final CharSequence correlationId) {

        logger.withCorrelationId(correlationId).withMdcEntry(ConnectivityMdcEntryKey.CONNECTION_ID, connectionId)
                .info("Starting Kafka consumer actor.");
        // ensure no previous consumer stays in memory
        stopConsumerActors();

        // start consumer actors
        connection().getSources().stream()
                .flatMap(GooglePubSubClientActor::consumerDataFromSource)
                .forEach(consumerData -> startGooglePubSubConsumer(consumerData, dryRun));
    }

    private static Stream<ConsumerData> consumerDataFromSource(final Source source) {
        return source.getAddresses().stream()
                .flatMap(sourceAddress ->
                        IntStream.range(0, source.getConsumerCount())
                                .mapToObj(i -> sourceAddress + "-" + i)
                                .map(addressWithIndex -> new ConsumerData(source, sourceAddress, addressWithIndex)));
    }

    private void startGooglePubSubConsumer(final ConsumerData consumerData, final boolean dryRun) {
        final GooglePubSubConsumerStreamFactory streamFactory =
                new GooglePubSubConsumerStreamFactory(propertiesFactory, consumerData, dryRun);
        final Props consumerActorProps = GooglePubSubConsumerActor.props(connection(), streamFactory, consumerData,
                getInboundMappingSink(), connectivityStatusResolver, connectivityConfig());
        final ActorRef consumerActor = startChildActorConflictFree(consumerData.getActorNamePrefix(), consumerActorProps);
        googlePubSubConsumerActors.add(consumerActor);
    }

    private void stopConsumerActors() {
        googlePubSubConsumerActors.forEach(consumerActor -> {
            logger.debug("Stopping child actor <{}>.", consumerActor.path());
            // shutdown using a message, so the actor can clean up first
            consumerActor.tell(GooglePubSubConsumerActor.GracefulStop.START, ActorRef.noSender());
        });
        googlePubSubConsumerActors.clear();
    }

    @Override
    protected void cleanupResourcesForConnection() {
        pendingStatusReportsFromStreams.clear();
        stopPublisherActor();
        stopConsumerActors();
    }

    @Override
    protected CompletionStage<Status.Status> startPublisherActor() {
        // wait for actor initialization to be sure any authentication errors are handled with backoff
        return new CompletableFuture<Status.Status>()
                .completeOnTimeout(DONE, googlePubSubConfig.getProducerConfig().getInitTimeoutSeconds(), TimeUnit.SECONDS);
    }

    @Override
    protected CompletionStage<Status.Status> startConsumerActors(@Nullable final ClientConnected clientConnected) {
        // wait for actor initialization to be sure any authentication errors are handled with backoff
        return new CompletableFuture<Status.Status>()
                .completeOnTimeout(DONE, googlePubSubConfig.getConsumerConfig().getInitTimeoutSeconds(), TimeUnit.SECONDS);
    }

    @Override
    protected Optional<ConnectionThrottlingConfig> getThrottlingConfig() {
        return Optional.empty(); // TODO
    }

    private void stopPublisherActor() {
        // TODO implement
    }

    private State<BaseClientState, BaseClientData> handleStatusReportFromChildren(final Status.Status status) {
        if (pendingStatusReportsFromStreams.contains(getSender())) {
            pendingStatusReportsFromStreams.remove(getSender());
            if (status instanceof Status.Failure failure) {
                final ConnectionFailure connectionFailure =
                        ConnectionFailure.of(null, failure.cause(), "child failed");
                getSelf().tell(connectionFailure, ActorRef.noSender());
            } else if (pendingStatusReportsFromStreams.isEmpty()) {
                // all children are ready; this client actor is connected.
                getSelf().tell((ClientConnected) Optional::empty, ActorRef.noSender());
            }
        }
        return stay();
    }

    private void completeTestConnectionFuture(final Status.Status testResult) {
        if (testConnectionFuture != null) {
            testConnectionFuture.complete(testResult);
        } else {
            // no future; test failed.
            final Exception exception = new IllegalStateException(
                    "Could not complete testing connection since the test was already completed or wasn't started.");
            getSelf().tell(new Status.Failure(exception), getSelf());
        }
    }

}
