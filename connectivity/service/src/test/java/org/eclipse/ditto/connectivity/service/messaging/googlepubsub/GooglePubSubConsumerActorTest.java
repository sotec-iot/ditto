package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.Props;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubMessage;
import org.apache.pekko.stream.javadsl.Sink;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.PayloadMapping;
import org.eclipse.ditto.connectivity.service.messaging.AbstractConsumerActorTest;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;

import java.util.Map;

/**
 * Tests {@code GooglePubSubConsumerActor}.
 */
public class GooglePubSubConsumerActorTest extends AbstractConsumerActorTest<PubSubMessage> {
    // TODO implement


    private static final Connection CONNECTION = TestConstants.createConnection();
    private static final String SUBSCRIPTION = "kafkapubsubtest.command";
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final String KEY = "key";
    private static final String CUSTOM_TOPIC = "the.topic";
    private static final String CUSTOM_KEY = "the.key";
    private static final String CUSTOM_TIMESTAMP = "the.timestamp";

    @Override
    protected void testHeaderMapping() {

    }

    @Override
    protected Props getConsumerActorProps(Sink<Object, NotUsed> inboundMappingSink, PayloadMapping payloadMapping) {
        return null;
    }

    @Override
    protected PubSubMessage getInboundMessage(String payload, Map.Entry<String, Object> header) {
        return null;
    }
}
