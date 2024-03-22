package org.eclipse.ditto.connectivity.service.config;

import com.typesafe.config.Config;
import org.eclipse.ditto.internal.utils.config.DefaultScopedConfig;
import org.eclipse.ditto.internal.utils.config.ScopedConfig;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class DefaultGooglePubSubConfig implements GooglePubSubConfig {

    private static final String CONFIG_PATH = "pubsub";

    private final GooglePubSubConsumerConfig consumerConfig;
    private final GooglePubSubProducerConfig producerConfig;

    private DefaultGooglePubSubConfig(final ScopedConfig googlePubSubScopedConfig) {
        consumerConfig = GooglePubSubConsumerConfig.of(googlePubSubScopedConfig);
        producerConfig = GooglePubSubProducerConfig.of(googlePubSubScopedConfig);
    }

    /**
     * Returns an instance of {@code DefaultGooglePubSubConfig} based on the settings of the specified Config.
     *
     * @param config is supposed to provide the Google PubSub config setting at {@value #CONFIG_PATH}.
     * @return the instance.
     * @throws org.eclipse.ditto.internal.utils.config.DittoConfigError if {@code config} is invalid.
     */
    public static DefaultGooglePubSubConfig of(final Config config) {
        return new DefaultGooglePubSubConfig(DefaultScopedConfig.newInstance(config, CONFIG_PATH));
    }

    @Override
    public GooglePubSubConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    @Override
    public GooglePubSubProducerConfig getProducerConfig() {
        return producerConfig;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DefaultGooglePubSubConfig that = (DefaultGooglePubSubConfig) o;
        return Objects.equals(consumerConfig, that.consumerConfig) &&
                Objects.equals(producerConfig, that.producerConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerConfig, producerConfig);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                "consumerConfig=" + consumerConfig +
                ", producerConfig=" + producerConfig +
                "]";
    }
}
