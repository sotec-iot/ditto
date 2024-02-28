package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.GenericTarget;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.service.config.ConnectivityConfig;
import org.eclipse.ditto.connectivity.service.messaging.BasePublisherActor;
import org.eclipse.ditto.connectivity.service.messaging.ConnectivityStatusResolver;
import org.eclipse.ditto.connectivity.service.messaging.PublishTarget;
import org.eclipse.ditto.connectivity.service.messaging.SendResult;

import javax.annotation.Nullable;
import java.util.concurrent.CompletionStage;

public class GooglePubSubPublisherActor extends BasePublisherActor {
    protected GooglePubSubPublisherActor(Connection connection, ConnectivityStatusResolver connectivityStatusResolver, ConnectivityConfig connectivityConfig) {
        super(connection, connectivityStatusResolver, connectivityConfig);
    }

    @Override
    protected void preEnhancement(ReceiveBuilder receiveBuilder) {

    }

    @Override
    protected void postEnhancement(ReceiveBuilder receiveBuilder) {

    }

    @Override
    protected PublishTarget toPublishTarget(GenericTarget target) {
        return null;
    }

    @Override
    protected CompletionStage<SendResult> publishMessage(Signal signal, @Nullable Target autoAckTarget, PublishTarget publishTarget, ExternalMessage message, int maxTotalMessageSize, int ackSizeQuota, @Nullable AuthorizationContext targetAuthorizationContext) {
        return null;
    }
}
