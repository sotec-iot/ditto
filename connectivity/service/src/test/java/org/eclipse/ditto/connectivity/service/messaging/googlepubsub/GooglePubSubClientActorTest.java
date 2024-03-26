package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.Done;
import org.apache.pekko.actor.*;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.eclipse.ditto.connectivity.api.BaseClientState;
import org.eclipse.ditto.connectivity.model.*;
import org.eclipse.ditto.connectivity.service.messaging.AbstractBaseClientActorTest;
import org.eclipse.ditto.connectivity.service.messaging.TestConstants;
import org.eclipse.ditto.connectivity.service.messaging.kafka.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.eclipse.ditto.connectivity.service.messaging.TestConstants.Authorization.AUTHORIZATION_CONTEXT;

@RunWith(MockitoJUnitRunner.class)
public final class GooglePubSubClientActorTest extends AbstractBaseClientActorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientActorTest.class);
    private static final Status.Success CONNECTED_SUCCESS = new Status.Success(BaseClientState.CONNECTED);
    private static final Status.Success DISCONNECTED_SUCCESS = new Status.Success(BaseClientState.DISCONNECTED);
    private static ActorSystem actorSystem;
    private static ServerSocket mockServer;
    private ConnectionId connectionId;
    private Connection connection;
    private static final String SUBSCRIPTION = "kafkapubsubtest.command";



    private static final Target TARGET = ConnectivityModelFactory.newTargetBuilder()
            .address(SUBSCRIPTION)
            .authorizationContext(AUTHORIZATION_CONTEXT)
            .qos(0)
            .topics(Topic.TWIN_EVENTS)
            .build();


    @BeforeClass
    public static void setUp() {
        actorSystem = ActorSystem.create("PekkoTestSystem", TestConstants.CONFIG);
        startMockServer();
    }

    private static void startMockServer() {
        mockServer = TestConstants.newMockServer();
        LOGGER.info("Started mock server on port {}", mockServer.getLocalPort());
    }

    @AfterClass
    public static void tearDown() {
        actorSystem.terminate();
        stopMockServer();
    }

    private static void stopMockServer() {
        if (null != mockServer) {
            try {
                mockServer.close();
                LOGGER.info("Successfully closed mock server.");
            } catch (final IOException e) {
                LOGGER.info("Got unexpected exception while closing the mock server.", e);
            }
        } else {
            LOGGER.info("Could not stop mock server as it unexpectedly was <null>.");
        }
    }

    @Before
    public void initializeConnection() {
        connectionId = TestConstants.createRandomConnectionId();

        final String projectId = "sotec-iot-core-dev";
        final Map<String, String> specificConfig = specificConfigWithProjectId(projectId);
        connection = ConnectivityModelFactory.newConnectionBuilder(connectionId, ConnectionType.PUBSUB,
                        ConnectivityStatus.CLOSED, "")
                .targets(singletonList(TARGET))
                .failoverEnabled(true)
                .specificConfig(specificConfig)
                .build();
    }

    @Test
    public void testConnect() {
        new TestKit(actorSystem) {{
            final TestProbe probe = new TestProbe(getSystem());
            final Props props = getGooglePubSubClientActorProps(probe.ref(), connection);


            final ActorRef googlePubSubClientActor = actorSystem.actorOf(props);

        }};
    }

    private Props getGooglePubSubClientActorProps(final ActorRef ref, final Connection connection) {
        return getGooglePubSubClientActorProps(ref, new Status.Success(Done.done()), connection);
    }

    private Props getGooglePubSubClientActorProps(final ActorRef ref, final Status.Status status,
                                           final Connection connection) {
        return null;
    }


    private static Map<String, String> specificConfigWithProjectId(final String... projectId) {
        final Map<String, String> specificConfig = new HashMap<>();
        specificConfig.put("projectid", String.join(",", projectId));
        return specificConfig;
    }


    @Override
    protected Connection getConnection(boolean isSecure) {
        return null;
    }

    @Override
    protected Props createClientActor(ActorRef proxyActor, Connection connection) {
        return null;
    }

    @Override
    protected ActorSystem getActorSystem() {
        return null;
    }

    private static void expectPublisherReceivedShutdownSignal(final TestProbe probe) {
        probe.expectMsg(GooglePubSubPublisherActor.GracefulStop.INSTANCE);
    }

    private static final class MockGooglePubSubPublisherActor extends AbstractActor {

        private final ActorRef target;

        private MockGooglePubSubPublisherActor(final ActorRef target, final Status.Status status) {
            this.target = target;
            getContext().getParent().tell(status, getSelf());
        }

        static Props props(final ActorRef target, final Status.Status status) {
            return Props.create(MockGooglePubSubPublisherActor.class, target, status);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(any -> target.forward(any, getContext())).build();
        }

    }

}
