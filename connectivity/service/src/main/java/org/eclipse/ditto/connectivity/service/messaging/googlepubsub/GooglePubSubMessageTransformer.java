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

import org.apache.pekko.stream.connectors.googlecloud.pubsub.PubSubMessage;
import org.apache.pekko.stream.connectors.googlecloud.pubsub.ReceivedMessage;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.api.ExternalMessageFactory;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.EnforcementFilterFactory;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.service.messaging.monitoring.ConnectionMonitor;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.internal.utils.pekko.logging.ThreadSafeDittoLogger;
import org.eclipse.ditto.internal.utils.tracing.DittoTracing;
import org.eclipse.ditto.internal.utils.tracing.span.SpanOperationName;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Transforms incoming messages from Google Pub/Sub to {@link org.eclipse.ditto.connectivity.api.ExternalMessage}.
 */
@Immutable
final class GooglePubSubMessageTransformer {

    private static final ThreadSafeDittoLogger LOGGER =
            DittoLoggerFactory.getThreadSafeLogger(GooglePubSubMessageTransformer.class);

    private final ConnectionId connectionId;
    private final Source source;
    private final String sourceAddress;
    private final EnforcementFilterFactory<Map<String, String>, Signal<?>> headerEnforcementFilterFactory;
    private final ConnectionMonitor inboundMonitor;


    GooglePubSubMessageTransformer(final ConnectionId connectionId, final Source source, final String sourceAddress,
                                   final EnforcementFilterFactory<Map<String, String>, Signal<?>> headerEnforcementFilterFactory,
                                   final ConnectionMonitor inboundMonitor) {
        this.connectionId = connectionId;
        this.source = source;
        this.sourceAddress = sourceAddress;
        this.headerEnforcementFilterFactory = headerEnforcementFilterFactory;
        this.inboundMonitor = inboundMonitor;
    }

    /**
     * Takes incoming Google Pub/Sub message and transforms the value to an {@link ExternalMessage}.
     *
     * @param receivedMessage the Google Pub/Sub message.
     * @return a value containing a {@link TransformationResult} that either contains an {@link ExternalMessage} in case
     * the transformation succeeded, or a {@link DittoRuntimeException} if it failed.
     * Could also be null if an unexpected Exception occurred which should result in the message being dropped as
     * automated recovery is expected.
     */
    @Nullable
    public TransformationResult<ReceivedMessage, ExternalMessage> transform(final ReceivedMessage receivedMessage) {
        final var message = receivedMessage.message();
        LOGGER.info("Received message from Google Pub/Sub: id[{}] data[{}]", message.messageId(), message.data());

        var messageAttributes = extractAttributesOfPubSubMessage(message);
        final var correlationId = messageAttributes
                .getOrDefault(DittoHeaderDefinition.CORRELATION_ID.getKey(), UUID.randomUUID().toString());

        final var startedSpan = DittoTracing.newPreparedSpan(messageAttributes, SpanOperationName.of("googlepubsub_consume"))
                .correlationId(correlationId)
                .connectionId(connectionId)
                .start();
        messageAttributes = startedSpan.propagateContext(messageAttributes);

        try {
            final var messageId = message.messageId();
            final var base64EncodedData = message.data().get();
            final var decodedBytes = Base64.getDecoder().decode(base64EncodedData);
            final var decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
            final var correlationIdScopedLogger = LOGGER.withCorrelationId(messageAttributes);
            correlationIdScopedLogger.info(
                    "Transforming incoming Google Pub/Sub message <{}> with attributes <{}> and messageId <{}>.",
                    decodedString, messageAttributes, messageId
            );

            final var externalMessage = ExternalMessageFactory.newExternalMessageBuilder(messageAttributes)
                    .withTextAndBytes(decodedString, decodedBytes)
                    .withAuthorizationContext(source.getAuthorizationContext())
//                    .withEnforcement(headerEnforcementFilterFactory.getFilter(messageAttributes)) TODO: potentially add it back. currently it works fine without enforcement
                    .withHeaderMapping(source.getHeaderMapping())
                    .withSourceAddress(sourceAddress)
                    .withPayloadMapping(source.getPayloadMapping())
                    .build();

            inboundMonitor.success(externalMessage);

            return TransformationSuccess.of(receivedMessage, externalMessage);
        } catch (final DittoRuntimeException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.withCorrelationId(e).info(
                        "Got DittoRuntimeException '{}' when command was parsed: {}", e.getErrorCode(),
                        e.getMessage());
            }
            startedSpan.tagAsFailed(e);
            return TransformationFailure.of(receivedMessage, new GooglePubSubTransformationException(
                    MessageFormat.format("Failed to transform {0} to {1}: {2}",
                            ReceivedMessage.class.getSimpleName(),
                            ExternalMessage.class.getSimpleName(),
                            e.getMessage()),
                    e,
                    messageAttributes
            ));
        } catch (final Exception e) {
            inboundMonitor.exception(messageAttributes, e);
            LOGGER.withCorrelationId(messageAttributes)
                    .error(String.format("Unexpected {%s}: {%s}", e.getClass().getName(), e.getMessage()), e);
            startedSpan.tagAsFailed(e);
            return null; // Drop message
        } finally {
            startedSpan.finish();
        }
    }

    private Map<String, String> extractAttributesOfPubSubMessage(final PubSubMessage message) {
        if (message.attributes() == null) {
            throw new RuntimeException("Google Pub/Sub message attributes is null");
        }

        final Map<String, String> attributes = new HashMap<>();
//        final scala.collection.immutable.Map<String, String> scalaMap = message.attributes().get();
//        scalaMap.foreach(entry -> attributes.put(entry._1(), entry._2()));
        attributes.computeIfAbsent(DittoHeaderDefinition.CORRELATION_ID.getKey(), key -> UUID.randomUUID().toString());
        return attributes;
    }
}
