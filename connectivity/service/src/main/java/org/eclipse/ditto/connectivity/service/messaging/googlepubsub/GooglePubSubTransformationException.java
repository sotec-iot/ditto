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



import javax.annotation.Nullable;
import java.io.Serial;
import java.util.Map;
import java.util.Objects;

/**
 * This exception is thrown to indicate that an error occurred during transformation of a Google Pub/Sub {@code ReceivedMessage}
 * to an {@code ExternalMessage} or vice versa.
 */
public class GooglePubSubTransformationException extends RuntimeException {

    @Serial private static final long serialVersionUID = 250822237737454668L; /** was copied from {@code MqttPublishTransformationException}  and incremented by 1 */

    private final Map<String, String> googlePubSubAttributes;

    /**
     * Constructs a {@code GooglePubSubTransformationException}.
     *
     * @param cause the error that caused the constructed exception.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public GooglePubSubTransformationException(final String detailMessage,
                                               final Throwable cause, Map<String, String> googlePubSubAttributes) {
        super(detailMessage, cause);
        this.googlePubSubAttributes = googlePubSubAttributes;
    }

    /**
     * Returns the attributes of the Google Pub/Sub {@code ReceivedMessage}.
     *
     * @return the possibly empty Google Pub/Sub {@code ReceivedMessage} attributes.
     */
    public Map<String, String> getGooglePubSubAttributes() {
        return googlePubSubAttributes;
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (GooglePubSubTransformationException) o;
        return Objects.equals(googlePubSubAttributes, that.googlePubSubAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(googlePubSubAttributes);
    }
}
