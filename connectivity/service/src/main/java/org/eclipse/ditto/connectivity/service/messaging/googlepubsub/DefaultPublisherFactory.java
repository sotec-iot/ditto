package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DefaultPublisherFactory implements PublisherFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPublisherFactory.class);

    @Override
    public Publisher newPublisher(String projectId, String topic) {
        try {
            final TopicName topicName = TopicName.of(projectId, topic);
            return Publisher.newBuilder(topicName)
                    .setEnableMessageOrdering(true)
                    .build();
        } catch (IOException e) {
            LOGGER.debug("Failed to create new Google Pub/Sub Publisher");
            throw new Error("Failed to create new Google Pub/Sub Publisher");
        }

    }
}
