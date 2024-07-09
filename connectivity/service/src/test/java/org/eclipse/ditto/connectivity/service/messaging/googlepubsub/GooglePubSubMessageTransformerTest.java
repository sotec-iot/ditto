package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubMessage;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class GooglePubSubMessageTransformerTest {

    private static final String CORRELATION_ID_KEY = DittoHeaderDefinition.CORRELATION_ID.getKey();

    @Test
    public void extractAttributesOfPubSubMessage_shouldReturnEmptyMap_whenMessageAttributesIsNull() {
        final var message = mock(PubSubMessage.class);
        final var transformer = new GooglePubSubMessageTransformer(null, null, null, null);

        assertThatThrownBy(() -> transformer.extractAttributesOfPubSubMessage(message))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Google Pub/Sub message attributes is null");
    }

    @Test
    public void extractAttributesOfPubSubMessage_shouldReturnMapWithCorrelationId_whenMessageAttributesIsEmpty() {
        final var attributes = Map.of("key1", "value1", "key2", "value2");
        final var message = new PubSubMessage(null, null, null, null).withAttributes(attributes);
        final var transformer = new GooglePubSubMessageTransformer(null, null, null, null);
        final var extractedAttributesOfPubSubMessage = transformer.extractAttributesOfPubSubMessage(message);

        assertThat(extractedAttributesOfPubSubMessage).containsKey(CORRELATION_ID_KEY);
        assertThat(extractedAttributesOfPubSubMessage.get(CORRELATION_ID_KEY)).isInstanceOf(String.class);
        assertThat(extractedAttributesOfPubSubMessage.get("key2")).isEqualTo("value2");
    }


}