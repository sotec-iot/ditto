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

import org.eclipse.ditto.connectivity.service.messaging.PublishTarget;

/**
 * A Google PubSub target (topic) to which messages can be published.
 */
public class GooglePubSubPublishTarget implements PublishTarget {


    private final String topic;

    private GooglePubSubPublishTarget(final String topic) {
        System.out.println("EG | In constructor of GooglePubSubPublishTarget");
        this.topic = topic;
    }

    static GooglePubSubPublishTarget fromTargetAddress(final String targetAddress) {
        System.out.println("EG | In constructor fromTargetAddress");
        return new GooglePubSubPublishTarget(targetAddress);
    }

    public String getTopic() {
        return topic;
    }
}
