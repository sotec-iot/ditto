package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.BaseConsumerActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.internal.utils.pekko.logging.ThreadSafeDittoLoggingAdapter;

public class GooglePubSubConsumerActor extends BaseConsumerActor {
    protected GooglePubSubConsumerActor(Connection connection, String sourceAddress, Sink<Object, ?> inboundMappingSink, Source source, ConnectivityStatusResolver connectivityStatusResolver, ConnectivityConfig connectivityConfig) {
        super(connection, sourceAddress, inboundMappingSink, source, connectivityStatusResolver, connectivityConfig);
    }

    @Override
    protected ThreadSafeDittoLoggingAdapter log() {
        return null;
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
