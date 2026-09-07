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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionConfigurationInvalidException;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.SourceBuilder;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.TargetBuilder;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;
import org.eclipse.ditto.placeholders.UnresolvedPlaceholderException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link GooglePubSubValidator}.
 */
public final class GooglePubSubValidatorTest {

    private static final GooglePubSubValidator UNDER_TEST = GooglePubSubValidator.getInstance();
    private static ActorSystem actorSystem;
    private static ConnectivityConfig connectivityConfig;

    @BeforeClass
    public static void setUp() {
        actorSystem = ActorSystem.create("PekkoTestSystem", TestConstants.CONFIG);
        connectivityConfig = TestConstants.CONNECTIVITY_CONFIG;
    }

    @AfterClass
    public static void tearDown() {
        if (actorSystem != null) {
            TestKit.shutdownActorSystem(actorSystem, scala.concurrent.duration.Duration.apply(5, TimeUnit.SECONDS),
                    false);
        }
    }

    @Test
    public void testConnectionType() {
        assertThat((CharSequence) UNDER_TEST.type()).isEqualTo(ConnectionType.PUBSUB);
    }

    @Test
    public void testValidationOfEnforcementWithThingIdFilter() {
        testValidationOfEnforcement("thing");
    }

    @Test
    public void testValidationOfEnforcementWithEntityIdFilter() {
        testValidationOfEnforcement("entity");
    }

    @Test
    public void testValidationOfEnforcementWithPolicyIdFilter() {
        testValidationOfEnforcement("policy");
    }

    @Test
    public void testValidationOfEnforcementWithFeatureIdFilter() {
        final Source source = newSourceBuilder()
                .enforcement(ConnectivityModelFactory.newEnforcement(
                        "{{ header:device_id }}",
                        "{{ feature:id }}"))
                .build();

        UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource");
    }

    private void testValidationOfEnforcement(final String filterPrefix) {
        final Source source = newSourceBuilder()
                .enforcement(ConnectivityModelFactory.newEnforcement(
                        "{{ header:device_id }}",
                        "{{ " + filterPrefix + ":id }}",
                        "{{ " + filterPrefix + ":name }}", "{{ " + filterPrefix + ":namespace }}"))
                .build();

        UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource");
    }

    @Test
    public void testValidHeaderMappingOnSource() {
        final Source source = newSourceBuilder()
                .headerMapping(TestConstants.HEADER_MAPPING)
                .build();

        UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource");
    }

    @Test
    public void testInvalidSourceHeaderMappingThrowsException() {
        final Map<String, String> mapping = new HashMap<>(TestConstants.HEADER_MAPPING.getMapping());
        mapping.put("thingId", "{{ thing:invalid }}");

        final Source source = newSourceBuilder()
                .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                .build();

        assertThatExceptionOfType(ConnectionConfigurationInvalidException.class)
                .isThrownBy(() -> UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource"))
                .withCauseInstanceOf(UnresolvedPlaceholderException.class);
    }

    @Test
    public void testInvalidEnforcementInputThrowsException() {
        final Source source = newSourceBuilder()
                .enforcement(ConnectivityModelFactory.newEnforcement("{{ thing:id }}", "{{ thing:namespace }}"))
                .build();

        assertThatExceptionOfType(ConnectionConfigurationInvalidException.class)
                .isThrownBy(() -> UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource"))
                .withCauseInstanceOf(UnresolvedPlaceholderException.class);
    }

    @Test
    public void testInvalidEnforcementFilterThrowsException() {
        final Source source = newSourceBuilder()
                .enforcement(ConnectivityModelFactory.newEnforcement(
                        "{{ header:device_id }}", "{{ header:ditto }}"))
                .build();

        assertThatExceptionOfType(ConnectionConfigurationInvalidException.class)
                .isThrownBy(() -> UNDER_TEST.validateSource(source, DittoHeaders.empty(), () -> "testSource"))
                .withCauseInstanceOf(UnresolvedPlaceholderException.class);
    }

    @Test
    public void testValidTargetAddress() {
        UNDER_TEST.validate(connectionWithTarget("projects/my-project/topics/my-topic"), DittoHeaders.empty(),
                actorSystem, connectivityConfig);
        UNDER_TEST.validate(connectionWithTarget("projects/{{thing:namespace}}/topics/{{thing:name}}"),
                DittoHeaders.empty(), actorSystem, connectivityConfig);
        UNDER_TEST.validate(connectionWithTarget("topics/{{thing:id}}"), DittoHeaders.empty(), actorSystem,
                connectivityConfig);
        UNDER_TEST.validate(connectionWithTarget("topics/{{topic:action}}"), DittoHeaders.empty(), actorSystem,
                connectivityConfig);
        UNDER_TEST.validate(connectionWithTarget("topics/{{header:some-header}}"), DittoHeaders.empty(),
                actorSystem, connectivityConfig);
    }

    @Test
    public void testInvalidTargetAddressThrowsException() {
        assertThatExceptionOfType(ConnectionConfigurationInvalidException.class)
                .isThrownBy(() -> UNDER_TEST.validate(connectionWithTarget("topics/{{invalid:placeholder}}"),
                        DittoHeaders.empty(), actorSystem, connectivityConfig))
                .withCauseInstanceOf(UnresolvedPlaceholderException.class);
    }

    @Test
    public void testValidTargetHeaderMapping() {
        final Target target = newTargetBuilder("projects/my-project/topics/my-topic")
                .headerMapping(TestConstants.HEADER_MAPPING)
                .build();

        UNDER_TEST.validateTarget(target, DittoHeaders.empty(), () -> "testTarget");
    }

    @Test
    public void testInvalidTargetHeaderMappingThrowsException() {
        final Map<String, String> mapping = new HashMap<>(TestConstants.HEADER_MAPPING.getMapping());
        mapping.put("thingId", "{{ thing:invalid }}");

        final Target target = newTargetBuilder("projects/my-project/topics/my-topic")
                .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                .build();

        assertThatExceptionOfType(ConnectionConfigurationInvalidException.class)
                .isThrownBy(() -> UNDER_TEST.validateTarget(target, DittoHeaders.empty(), () -> "testTarget"))
                .withCauseInstanceOf(UnresolvedPlaceholderException.class);
    }

    private static SourceBuilder newSourceBuilder() {
        return ConnectivityModelFactory.newSourceBuilder()
                .address("projects/my-project/subscriptions/my-sub")
                .authorizationContext(TestConstants.Authorization.AUTHORIZATION_CONTEXT);
    }

    private static TargetBuilder newTargetBuilder(final String address) {
        return ConnectivityModelFactory.newTargetBuilder()
                .address(address)
                .authorizationContext(TestConstants.Authorization.AUTHORIZATION_CONTEXT)
                .topics(Topic.TWIN_EVENTS);
    }

    private static Connection connectionWithTarget(final String targetAddress) {
        return ConnectivityModelFactory.newConnectionBuilder(TestConstants.createRandomConnectionId(),
                        ConnectionType.PUBSUB, ConnectivityStatus.OPEN, "")
                .targets(singletonList(newTargetBuilder(targetAddress).build()))
                .build();
    }
}
