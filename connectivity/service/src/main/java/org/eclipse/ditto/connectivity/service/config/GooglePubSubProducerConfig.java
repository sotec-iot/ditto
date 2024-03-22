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
import org.eclipse.ditto.internal.utils.config.KnownConfigValue;

import javax.annotation.concurrent.Immutable;

@Immutable
public interface GooglePubSubProducerConfig {

    /**
     * @return timeout before the consumer is initialized and considered "ready".
     */
    long getInitTimeoutSeconds();

    /**
     * Returns an instance of {@code GooglePubSubProducerConfig} based on the settings of the specified Config.
     *
     * @param config is supposed to provide the settings.
     * @return the instance.
     * @throws org.eclipse.ditto.internal.utils.config.DittoConfigError if {@code config} is invalid.
     */
    static GooglePubSubProducerConfig of(final Config config) {
        return DefaultGooglePubSubProducerConfig.of(config);
    }

    enum ConfigValue implements KnownConfigValue {

        INIT_TIMEOUT_SECONDS("init-timeout-seconds", 3);

        private final String path;
        private final Object defaultValue;

        ConfigValue(final String thePath, final Object theDefaultValue) {
            path = thePath;
            defaultValue = theDefaultValue;
        }


        @Override
        public Object getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String getConfigPath() {
            return path;
        }
    }

}
