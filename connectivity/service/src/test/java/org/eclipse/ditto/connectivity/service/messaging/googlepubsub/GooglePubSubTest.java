package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import com.google.common.collect.Lists;
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Cancellable;
import org.apache.pekko.actor.Props;
import org.apache.pekko.stream.connectors.google.GoogleAttributes;
import org.apache.pekko.stream.connectors.google.GoogleSettings;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.*;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.javadsl.GooglePubSub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

@RunWith(MockitoJUnitRunner.class)
public class GooglePubSubTest {


    @Test
    public void testDependenciesGoogleCommonsAndPubSub() throws InterruptedException {
        ActorSystem system = ActorSystem.create();
        final TestProbe probe = new TestProbe(system);

        // Setting up Google Common and Pub/Sub
        GoogleSettings defaultSettings = GoogleSettings.create(system);
        org.apache.pekko.stream.javadsl.Source.fromMaterializer(
                (mat, attr) -> {
                    GoogleSettings settings = GoogleAttributes.resolveSettings(mat, attr);
                    return org.apache.pekko.stream.javadsl.Source.empty();
                });
        System.out.println(defaultSettings.getProjectId());
        PubSubConfig config = PubSubConfig.create();
        String topic = "pub.sub.test";
        String subscription = "pub.sub.test.subscription";

        // CONSUMING FROM Google Topic pub.sub.test using Subscription pub.sub.test.subscription
        org.apache.pekko.stream.javadsl.Source<ReceivedMessage, Cancellable> subscriptionSource =
                GooglePubSub.subscribe(subscription, config);

        Sink<AcknowledgeRequest, CompletionStage<Done>> ackSink =
                GooglePubSub.acknowledge(subscription, config);

        CompletionStage<Done> consumedConfirmation =
                subscriptionSource
                        .map(
                                message -> {
                                    System.out.println("consumed message: " + message.toString());
                                    return message.ackId();
                                })
                        .groupedWithin(10, Duration.ofSeconds(1))
                        .map(AcknowledgeRequest::create)
                        .runWith(ackSink, system);

        consumedConfirmation.thenRun(() ->
                System.out.println("Alle Nachrichten wurden erfolgreich konsumiert und bestätigt.")
        );

        // PUBLISHING TO pub.sub.test
        PublishMessage publishMessage =
                PublishMessage.create(new String(Base64.getEncoder().encode("Hello Google!".getBytes())));
        PublishRequest publishRequest = PublishRequest.create(Lists.newArrayList(publishMessage));
        org.apache.pekko.stream.javadsl.Source<PublishRequest, NotUsed> source =
                org.apache.pekko.stream.javadsl.Source.single(publishRequest);
        Flow<PublishRequest, List<String>, NotUsed> publishFlow =
                GooglePubSub.publish(topic, config, 1);

        CompletionStage<List<String>> publishedMessageIds =
                source.via(publishFlow).runWith(Sink.head(), system);

        publishedMessageIds.thenAccept(publishedIds -> {
            if (publishedIds.isEmpty()) {
                System.out.println("No message published");
            } else {
                System.out.println("Published message ids: " + publishedIds);
            }
        });
        Thread.sleep(60000);
    }
}
