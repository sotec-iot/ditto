package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;


import com.google.cloud.pubsub.v1.Publisher;

/**
 * Creates Google {@link Publisher}s.
 */
@FunctionalInterface
interface PublisherFactory {

    /**
     * Create a publisher of Google Pub/Sub messages.
     *
     * @return the publisher.
     */
    Publisher newPublisher(final String projectId, final String topic);

}
