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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.eclipse.ditto.connectivity.service.messaging.PublishTarget;

/**
 * A Google PubSub target (topic) to which messages can be published.
 */
public class GooglePubSubPublishTarget implements PublishTarget {

    private static final Pattern FULL_TOPIC_PATTERN = Pattern.compile("^projects/([^/]+)/topics/(.+)$");
    private static final Pattern SHORT_TOPIC_PATTERN = Pattern.compile("^topics/(.+)$");

    @Nullable
    private final String projectId;
    private final String topic;

    private GooglePubSubPublishTarget(@Nullable final String projectId, final String topic) {
        this.projectId = projectId;
        this.topic = topic;
    }

    static GooglePubSubPublishTarget fromTargetAddress(final String targetAddress) {
        final Matcher fullMatcher = FULL_TOPIC_PATTERN.matcher(targetAddress);
        if (fullMatcher.matches()) {
            return new GooglePubSubPublishTarget(fullMatcher.group(1), fullMatcher.group(2));
        }
        final Matcher shortMatcher = SHORT_TOPIC_PATTERN.matcher(targetAddress);
        if (shortMatcher.matches()) {
            return new GooglePubSubPublishTarget(null, shortMatcher.group(1));
        }
        return new GooglePubSubPublishTarget(null, targetAddress);
    }

    public Optional<String> getProjectId() {
        return Optional.ofNullable(projectId);
    }

    public String getTopic() {
        return topic;
    }
}
