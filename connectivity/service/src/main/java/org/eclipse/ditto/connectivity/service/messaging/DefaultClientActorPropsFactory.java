/*
 * Copyright (c) 2017 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.connectivity.service.messaging;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.service.messaging.amqp.AmqpClientActor;
import org.eclipse.ditto.connectivity.service.messaging.googlepubsub.GooglePubSubClientActor;
import org.eclipse.ditto.connectivity.service.messaging.googlepubsub.GooglePubSubConnectionFactory;
import org.eclipse.ditto.connectivity.service.messaging.hono.HonoConnectionFactory;
import org.eclipse.ditto.connectivity.service.messaging.httppush.HttpPushClientActor;
import org.eclipse.ditto.connectivity.service.messaging.kafka.KafkaClientActor;
import org.eclipse.ditto.connectivity.service.messaging.mqtt.hivemq.MqttClientActor;
import org.eclipse.ditto.connectivity.service.messaging.rabbitmq.RabbitMQClientActor;

import com.typesafe.config.Config;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;

/**
 * The default implementation of {@link ClientActorPropsFactory}. Singleton which is created just once
 * and otherwise returns the already created instance.
 */
@Immutable
public final class DefaultClientActorPropsFactory implements ClientActorPropsFactory {

    private final HonoConnectionFactory honoConnectionFactory;

    private final GooglePubSubConnectionFactory googlePubSubConnectionFactory;

    public DefaultClientActorPropsFactory(final ActorSystem actorSystem, final Config config) {
        System.out.println("In Constructor of DefaultClientActorPropsFactory");
        honoConnectionFactory = HonoConnectionFactory.get(actorSystem, config);
        googlePubSubConnectionFactory = GooglePubSubConnectionFactory.get(actorSystem, config);
    }

    @Override
    public Props getActorPropsForType(final Connection connection,
            final ActorRef commandForwarderActor,
            final ActorRef connectionActor,
            final ActorSystem actorSystem,
            final DittoHeaders dittoHeaders,
            final Config connectivityConfigOverwrites) {
        System.out.println("In getActorPropsForType of DefaultClientActorPropsFactory");

        return switch (connection.getConnectionType()) {
            case AMQP_091 -> RabbitMQClientActor.props(connection,
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
            case AMQP_10 -> AmqpClientActor.props(connection,
                    commandForwarderActor,
                    connectionActor,
                    connectivityConfigOverwrites,
                    actorSystem,
                    dittoHeaders);
            case MQTT, MQTT_5 -> MqttClientActor.props(connection,
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
            case KAFKA -> KafkaClientActor.props(connection,
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
            case HTTP_PUSH -> HttpPushClientActor.props(connection,
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
            case HONO -> KafkaClientActor.props(getResolvedHonoConnectionOrThrow(connection, dittoHeaders),
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
            case PUBSUB -> GooglePubSubClientActor.props(getResolvedGooglePubSubConnectionOrThrow(connection, dittoHeaders),
                    commandForwarderActor,
                    connectionActor,
                    dittoHeaders,
                    connectivityConfigOverwrites);
        };
    }

    private Connection getResolvedHonoConnectionOrThrow(final Connection connection, final DittoHeaders dittoHeaders) {
        System.out.println("In getResolvedHonoConnectionOrThrow"); // TODO remove
        try {
            return honoConnectionFactory.getHonoConnection(connection);
        } catch (final DittoRuntimeException e) {
            throw e.setDittoHeaders(dittoHeaders);
        }
    }

    private Connection getResolvedGooglePubSubConnectionOrThrow(final Connection connection, final DittoHeaders dittoHeaders) {
        System.out.println("In getResolvedGooglePubSubConnectionOrThrow"); // TODO remove
        try {
            return googlePubSubConnectionFactory.getGooglePubSubConnection(connection);
        } catch (final DittoRuntimeException e) {
            throw e.setDittoHeaders(dittoHeaders);
        }
    }

}
