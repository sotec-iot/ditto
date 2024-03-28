package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubConfig;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.PublishRequest;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.service.messaging.AbstractBaseClientActorTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

@RunWith(MockitoJUnitRunner.class)
public class PublishMessageWithPekkoConnectorTest extends AbstractBaseClientActorTest {

    @Test
    public void publishMessageWithPekkoPubSubConnector() {
        ActorSystem system = ActorSystem.create();

        PubSubConfig config = PubSubConfig.create();
        String topic = "kafkapubsubtest.command";
        String subscription = "kafkapubsubtest.command";
        PublishMessage publishMessage =
                PublishMessage.create(new String(Base64.getEncoder().encode("Hello Google!".getBytes())));
        PublishRequest publishRequest = PublishRequest.create(List.of(publishMessage));

        Source<PublishRequest, NotUsed> source = Source.single(publishRequest);

        Flow<PublishRequest, List<String>, NotUsed> publishFlow =
                GooglePubSub.publish(topic, config, 1);

        CompletionStage<List<List<String>>> publishedMessageIds =
                source.via(publishFlow).runWith(Sink.seq(), system);

        publishedMessageIds.thenApply(x -> {
            x.stream().forEach(strings -> {
                for (String s : strings) {
                    System.out.println(s);
                }
            });
            return null;
        });
    }

    @Override
    protected Connection getConnection(boolean isSecure) {
        return null;
    }

    @Override
    protected Props createClientActor(ActorRef proxyActor, Connection connection) {
        return null;
    }

    @Override
    protected ActorSystem getActorSystem() {
        return null;
    }
}
