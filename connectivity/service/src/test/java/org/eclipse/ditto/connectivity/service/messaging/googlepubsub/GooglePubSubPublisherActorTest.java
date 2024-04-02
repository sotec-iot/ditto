package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pekko.actor.Props;
import org.apache.pekko.testkit.TestProbe;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.DittoConnectivityConfig;
import org.eclipse.ditto.connectivity.service.config.GooglePubSubProducerConfig;
import org.eclipse.ditto.connectivity.service.messaging.AbstractPublisherActorTest;
import org.eclipse.ditto.internal.utils.config.DefaultScopedConfig;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class GooglePubSubPublisherActorTest extends AbstractPublisherActorTest {
    // TODO implement

    private static final String TARGET_TOPIC = "anyTopic";
    private static final String OUTBOUND_ADDRESS = TARGET_TOPIC + "/keyA";

    private final Queue<ProducerRecord<String, ByteBuffer>> published = new ConcurrentLinkedQueue<>();

    private final DittoConnectivityConfig connectivityConfig =
            DittoConnectivityConfig.of(DefaultScopedConfig.dittoScoped(CONFIG));

    private final GooglePubSubProducerConfig googlePubSubConfig = connectivityConfig
            .getConnectionConfig()
            .getGooglePubSubConfig()
            .getProducerConfig();

    @Override
    protected String getOutboundAddress() {
        return null;
    }

    @Override
    protected void setupMocks(TestProbe probe) throws Exception {

    }

    @Override
    protected Props getPublisherActorProps() {
        return null;
    }

    @Override
    protected Target decorateTarget(Target target) {
        return null;
    }

    @Override
    protected void verifyPublishedMessage() throws Exception {

    }

    @Override
    protected void verifyPublishedMessageToReplyTarget() throws Exception {

    }

    @Override
    protected void verifyAcknowledgements(Supplier<Acknowledgements> ackSupplier) throws Exception {

    }
}
