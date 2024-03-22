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
package org.eclipse.ditto.connectivity.service.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 * This class is the default implementation of {@link GooglePubSubConsumerConfig}.
 */
@Immutable
public class DefaultGooglePubSubConsumerConfig implements GooglePubSubConsumerConfig {

    private static final String CONFIG_PATH = "consumer";

    private final long initTimeoutSeconds;

    // TODO check what PekkoConnectorsConfig is used for an include if needed
    // TODO implement equals, toString, hascode

    private DefaultGooglePubSubConsumerConfig(final Config googlePubSubConsumerScopedConfig) {
        initTimeoutSeconds = googlePubSubConsumerScopedConfig.getLong(KafkaConsumerConfig.ConfigValue.INIT_TIMEOUT_SECONDS.getConfigPath());
    }

    /**
     * Returns an instance of {@code DefaultGooglePubSubConsumerConfig} based on the settings of the specified Config.
     *
     * @param config is supposed to provide the Google PubSub config setting.
     * @return the instance.
     * @throws org.eclipse.ditto.internal.utils.config.DittoConfigError if {@code config} is invalid.
     */
    public static DefaultGooglePubSubConsumerConfig of(final Config config) {
        return new DefaultGooglePubSubConsumerConfig(getConfigOrEmpty(config, CONFIG_PATH));
    }

    private static Config getConfigOrEmpty(final Config config, final String configKey) {
        return config.hasPath(configKey) ? config.getConfig(configKey) : ConfigFactory.empty();
    }

    @Override
    public long getInitTimeoutSeconds() {
        return initTimeoutSeconds;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DefaultGooglePubSubConsumerConfig that = (DefaultGooglePubSubConsumerConfig) o;
        return Objects.equals(initTimeoutSeconds, that.initTimeoutSeconds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initTimeoutSeconds);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                "initTimeoutSeconds=" + initTimeoutSeconds +
                "]";
    }

}
