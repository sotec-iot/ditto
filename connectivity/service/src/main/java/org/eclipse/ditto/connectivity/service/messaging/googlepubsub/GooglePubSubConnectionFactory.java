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

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.internal.utils.extension.DittoExtensionIds;
import org.eclipse.ditto.internal.utils.extension.DittoExtensionPoint;

import java.text.MessageFormat;
import java.util.*;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkArgument;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

/**
 * Base implementation of a factory for getting a Google Pub/Sub {@link Connection}.
 * The Connection this factory supplies is based on a provided Connection with adjustments of
 * <ul>
 *     <li>the base URI,</li>
 *     <li>the "validate certificates" flag,</li>
 *     <li>the specific config including SASL mechanism, bootstrap server URIs and group ID,</li>
 *     <li>the credentials and</li>
 *     <li>the sources and targets.</li>
 * </ul>
 */
public abstract class GooglePubSubConnectionFactory implements DittoExtensionPoint {

    /**
     * The name of the property in the {@code specificConfig} object containing the GCP project ID.
     */
    protected static final String SPEC_CONFIG_PROJECT_ID = "projectId";

    private String projectId;

    /**
     * Constructs a {@code GooglePubSubConnectionFactory}.
     */
    protected GooglePubSubConnectionFactory() {
        super();
    }

    /**
     * Loads the implementation of {@code GooglePubSubConnectionFactory} which is configured for the specified
     * {@code ActorSystem}.
     *
     * @param actorSystem the actorSystem in which the {@code GooglePubSubConnectionFactory} should be loaded.
     * @param config      the configuration for this extension.
     * @return the {@code GooglePubSubConnectionFactory} implementation.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static GooglePubSubConnectionFactory get(final ActorSystem actorSystem, final Config config) {
        checkNotNull(actorSystem, "actorSystem");
        checkNotNull(config, "config");

        return DittoExtensionIds.get(actorSystem)
                .computeIfAbsent(
                        DittoExtensionPoint.ExtensionId.ExtensionIdConfig.of(GooglePubSubConnectionFactory.class,
                                config,
                                ExtensionId.CONFIG_KEY),
                        ExtensionId::new
                )
                .get(actorSystem);
    }

    /**
     * Returns a proper Google Pub/Sub Connection for the Connection that was used to create this factory instance.
     *
     * @param connection the connection that serves as base for the Google Pub/Sub connection this factory returns.
     * @return the Google Pub/Sub Connection.
     * @throws NullPointerException                                          if {@code connection} is {@code null}.
     * @throws IllegalArgumentException                                      if the type of {@code connection} is not {@link ConnectionType#PUBSUB};
     * @throws org.eclipse.ditto.base.model.exceptions.DittoRuntimeException if converting {@code connection} to a
     *                                                                       Google Pub/Sub connection failed for some reason.
     */
    public Connection getGooglePubSubConnection(final Connection connection) {
        System.out.println("In getGooglePubSubConnection of factory"); // TODO remove
        checkArgument(
                checkNotNull(connection, "connection"),
                arg -> ConnectionType.PUBSUB == arg.getConnectionType(),
                () -> MessageFormat.format("Expected type of connection to be <{0}> but it was <{1}>.",
                        ConnectionType.PUBSUB,
                        connection.getConnectionType())
        );

        projectId = connection.getSpecificConfig()
                .getOrDefault(SPEC_CONFIG_PROJECT_ID, connection.getId().toString());

        preConversion(connection);

        return ConnectivityModelFactory.newConnectionBuilder(connection)
                .uri("") // Setting uri to empty string because NULL is not allowed
//                .validateCertificate(isValidateCertificates()) TODO check if have to be included - answer seems no
//                .trustedCertificates(getTrustedCertificates()) TODO check if have to be included - answer seems no
                .specificConfig(makeupSpecificConfig(connection))
                .setSources(connection.getSources())
                .setTargets(connection.getTargets())
                .build();
    }

    /**
     * User overridable callback.
     * This method is called before the actual conversion of the specified {@code Connection} is performed.
     * Empty default implementation.
     *
     * @param googlePubSubConnection the connection that is guaranteed to have type {@link ConnectionType#PUBSUB}.
     */
    protected void preConversion(final Connection googlePubSubConnection) {
        // Do nothing by default.
    }

    /**
     * Get the project ID associated with the connection.
     *
     * @return The project ID.
     */
    protected String getProjectId() {
        if (projectId == null) {
            throw new IllegalStateException("getProjectId invoked before project ID got determined");
        }
        return projectId;
    }

    private Map<String, String> makeupSpecificConfig(final Connection connection) {
        return Map.of("projectId", connection.getSpecificConfig().get("projectId"));
    }

    public static final class ExtensionId extends DittoExtensionPoint.ExtensionId<GooglePubSubConnectionFactory> {

        private static final String CONFIG_KEY = "googlepubsub-connection-factory";

        private ExtensionId(final ExtensionIdConfig<GooglePubSubConnectionFactory> extensionIdConfig) {
            super(extensionIdConfig);
        }

        @Override
        protected String getConfigKey() {
            return CONFIG_KEY;
        }

    }

}
