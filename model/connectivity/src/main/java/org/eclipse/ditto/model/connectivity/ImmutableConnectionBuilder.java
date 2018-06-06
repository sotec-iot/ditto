/*
 * Copyright (c) 2017 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/epl-2.0/index.php
 * Contributors:
 *    Bosch Software Innovations GmbH - initial contribution
 *
 */
package org.eclipse.ditto.model.connectivity;

import static org.eclipse.ditto.model.base.common.ConditionChecker.checkArgument;
import static org.eclipse.ditto.model.base.common.ConditionChecker.checkNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.eclipse.ditto.model.base.auth.AuthorizationContext;

/**
 * Builder for {@code ImmutableConnection}.
 */
final class ImmutableConnectionBuilder implements ConnectionBuilder {

    // final:
    final ConnectionType connectionType;

    // changeable:
    String id;
    AuthorizationContext authorizationContext;
    ConnectionStatus connectionStatus;
    Set<String> tags = new HashSet<>();
    String uri;

    // optional:
    boolean failoverEnabled = true;
    boolean validateCertificate = true;
    final Set<Source> sources = new HashSet<>();
    final Set<Target> targets = new HashSet<>();
    int clientCount = 1;
    int processorPoolSize = 5;
    final Map<String, String> specificConfig = new HashMap<>();
    @Nullable MappingContext mappingContext = null;
    @Nullable String name = null;

    private ImmutableConnectionBuilder(final String id,
            final ConnectionType connectionType,
            final ConnectionStatus connectionStatus,
            final String uri,
            final AuthorizationContext authorizationContext) {

        this.connectionType = checkNotNull(connectionType, "Connection Type");
        this.id = checkNotNull(id, "ID");
        this.authorizationContext = checkNotNull(authorizationContext, "Authorization Context");
        this.connectionStatus = checkNotNull(connectionStatus, "Connection Status");
        this.uri = checkNotNull(uri, "URI");
    }

    /**
     * Instantiates a new {@code ImmutableConnectionBuilder}.
     *
     * @param id the connection id
     * @param connectionType the connection type
     * @param connectionStatus the connection status
     * @param uri the uri
     * @param authorizationContext the authorization context
     * @return new instance of {@code ImmutableConnectionBuilder}
     */
    static ConnectionBuilder of(final String id,
            final ConnectionType connectionType,
            final ConnectionStatus connectionStatus,
            final String uri,
            final AuthorizationContext authorizationContext) {

        return new ImmutableConnectionBuilder(id, connectionType, connectionStatus, uri, authorizationContext);
    }

    /**
     * Instantiates a new {@code ImmutableConnectionBuilder}.
     *
     * @param connection the connection to use for initializing the builder
     * @return new instance of {@code ImmutableConnectionBuilder}
     */
    static ConnectionBuilder of(final Connection connection) {
        final ImmutableConnectionBuilder connectionBuilder =
                new ImmutableConnectionBuilder(connection.getId(), connection.getConnectionType(),
                        connection.getConnectionStatus(), connection.getUri(), connection.getAuthorizationContext());
        connectionBuilder.failoverEnabled(connection.isFailoverEnabled());
        connectionBuilder.validateCertificate(connection.isValidateCertificates());
        connectionBuilder.processorPoolSize(connection.getProcessorPoolSize());
        connectionBuilder.sources(connection.getSources());
        connectionBuilder.targets(connection.getTargets());
        connectionBuilder.clientCount(connection.getClientCount());
        connectionBuilder.specificConfig(connection.getSpecificConfig());
        connectionBuilder.mappingContext(connection.getMappingContext().orElse(null));
        connectionBuilder.name(connection.getName().orElse(null));
        connectionBuilder.tags(connection.getTags());
        return connectionBuilder;
    }

    @Override
    public ConnectionBuilder id(final String id) {
        this.id = id;
        return this;
    }

    @Override
    public ConnectionBuilder name(@Nullable final String name) {
        this.name = name;
        return this;
    }

    @Override
    public ConnectionBuilder authorizationContext(final AuthorizationContext authorizationContext) {
        this.authorizationContext = authorizationContext;
        return this;
    }

    @Override
    public ConnectionBuilder uri(final String uri) {
        this.uri = uri;
        return this;
    }

    @Override
    public ConnectionBuilder connectionStatus(final ConnectionStatus connectionStatus) {
        this.connectionStatus = connectionStatus;
        return this;
    }

    @Override
    public ConnectionBuilder failoverEnabled(final boolean failoverEnabled) {
        this.failoverEnabled = failoverEnabled;
        return this;
    }

    @Override
    public ConnectionBuilder validateCertificate(final boolean validateCertificate) {
        this.validateCertificate = validateCertificate;
        return this;
    }

    @Override
    public ConnectionBuilder processorPoolSize(final int processorPoolSize) {
        checkArgument(processorPoolSize, ps -> ps > 0, () -> "The consumer count must be positive!");
        this.processorPoolSize = processorPoolSize;
        return this;
    }

    @Override
    public ConnectionBuilder sources(final Set<Source> sources) {
        checkNotNull(sources, "Sources");
        this.sources.addAll(sources);
        return this;
    }

    @Override
    public ConnectionBuilder targets(final Set<Target> targets) {
        checkNotNull(targets, "Targets");
        this.targets.addAll(targets);
        return this;
    }

    @Override
    public ConnectionBuilder clientCount(final int clientCount) {
        checkArgument(clientCount, ps -> ps > 0, () -> "The client count must be > 0!");
        this.clientCount = clientCount;
        return this;
    }

    @Override
    public ConnectionBuilder specificConfig(final Map<String, String> specificConfig) {
        checkNotNull(specificConfig, "Specific Config");
        this.specificConfig.putAll(specificConfig);
        return this;
    }

    @Override
    public ConnectionBuilder mappingContext(@Nullable final MappingContext mappingContext) {
        this.mappingContext = mappingContext;
        return this;
    }

    @Override
    public ConnectionBuilder tags(final Collection<String> tags) {
        this.tags = new HashSet<>(tags);
        return this;
    }

    @Override
    public ConnectionBuilder tag(final String tag) {
        tags.add(tag);
        return this;
    }

    @Override
    public Connection build() {
        return new ImmutableConnection(this);
    }

}
