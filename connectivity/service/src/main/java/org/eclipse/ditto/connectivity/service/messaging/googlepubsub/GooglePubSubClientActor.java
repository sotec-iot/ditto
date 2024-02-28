package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.signals.commands.modify.TestConnection;
import org.eclipse.ditto.connectivity.service.messaging.BaseClientActor;

import javax.annotation.Nullable;
import java.util.concurrent.CompletionStage;

public class GooglePubSubClientActor extends BaseClientActor {

    @SuppressWarnings("unused") // used by `props` via reflection
    protected GooglePubSubClientActor(Connection connection, ActorRef commandForwarderActor, ActorRef connectionActor, DittoHeaders dittoHeaders, Config connectivityConfigOverwrites) {
        super(connection, commandForwarderActor, connectionActor, dittoHeaders, connectivityConfigOverwrites);
    }

    @Override
    protected CompletionStage<Status.Status> doTestConnection(TestConnection testConnectionCommand) {
        return null;
    }

    @Override
    protected CompletionStage<Void> stopConsuming() {
        return null;
    }

    @Override
    protected void cleanupResourcesForConnection() {

    }

    @Override
    protected void doConnectClient(Connection connection, @Nullable ActorRef origin) {
        connectClient(false, connection.getId(), null);
    }

    private void connectClient(final boolean dryRun, final ConnectionId connectionId,
                               @Nullable final CharSequence correlationId) {
        startPublisher(dryRun, connectionId, correlationId);
        startMessageConsumers(dryRun, connectionId, correlationId);
    }

    private void startPublisher(final boolean dryRun, final ConnectionId connectionId,
                                @Nullable final CharSequence correlationId) {
    }

    private void startMessageConsumers(final boolean dryRun, final ConnectionId connectionId,
                                       @Nullable final CharSequence correlationId) {
    }

    @Override
    protected void doDisconnectClient(Connection connection, @Nullable ActorRef origin, boolean shutdownAfterDisconnect) {

    }

    @Nullable
    @Override
    protected ActorRef getPublisherActor() {
        return null;
    }

    @Override
    protected CompletionStage<Status.Status> startPublisherActor() {
        return null;
    }



    /**
     * Creates a Pekko configuration object for this actor.
     *
     * @param connection the connection.
     * @param commandForwarderActor the actor used to send signals into the ditto cluster.
     * @param connectionActor the connectionPersistenceActor which created this client.
     * @param dittoHeaders headers of the command that caused this actor to be created.
     * @param connectivityConfigOverwrites the overwrites for the connectivity config for the given connection.
     * @return the Pekko configuration Props object.
     */
    public static Props props(final Connection connection,
                              final ActorRef commandForwarderActor,
                              final ActorRef connectionActor,
                              final DittoHeaders dittoHeaders,
                              final Config connectivityConfigOverwrites) {

        return Props.create(GooglePubSubClientActor.class, connection, commandForwarderActor,
                connectionActor, dittoHeaders, connectivityConfigOverwrites);
    }
}
