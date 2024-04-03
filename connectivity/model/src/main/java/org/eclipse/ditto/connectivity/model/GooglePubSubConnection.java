package org.eclipse.ditto.connectivity.model;

import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonParseException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.text.MessageFormat;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

/**
 * Immutable implementation of {@link AbstractConnection} of {@link ConnectionType} Pub/Sub.
 *
 * @since 3.2.0
 */
@Immutable
final class GooglePubSubConnection extends AbstractConnection {

    private GooglePubSubConnection(final Builder builder) {
        super(builder);
        System.out.println("In Constructor of GooglePubSubConnection");
    }

    @Override
    ConnectionUri getConnectionUri(@Nullable String builderConnectionUri) {
        return ConnectionUri.of(builderConnectionUri);
    }

    static ConnectionType getConnectionTypeOrThrow(final JsonObject jsonObject) {
        final String readConnectionType = jsonObject.getValueOrThrow(JsonFields.CONNECTION_TYPE);
        return ConnectionType.forName(readConnectionType).filter(type -> type == ConnectionType.PUBSUB)
                .orElseThrow(() -> JsonParseException.newBuilder()
                        .message(MessageFormat.format("Connection type <{0}> is invalid! Connection type must be of" +
                                " type <{1}>.", readConnectionType, ConnectionType.PUBSUB))
                        .build());
    }

    /**
     * Returns a new {@code ConnectionBuilder} object.
     *
     * @param id the connection ID.
     * @param connectionType the connection type.
     * @param connectionStatus the connection status.
     * @param uri the URI.
     * @return new instance of {@code ConnectionBuilder}.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static ConnectionBuilder getBuilder(final ConnectionId id,
                                               final ConnectionType connectionType,
                                               final ConnectivityStatus connectionStatus,
                                               final String uri) {

        return new Builder(connectionType)
                .id(id)
                .connectionStatus(connectionStatus)
                .uri(ConnectionUri.of(uri).toString());
    }

    /**
     * Returns a new {@code ConnectionBuilder} object.
     *
     * @param connection the connection to use for initializing the builder.
     * @return new instance of {@code ConnectionBuilder}.
     * @throws NullPointerException if {@code connection} is {@code null}.
     */
    public static ConnectionBuilder getBuilder(final Connection connection) {
        checkNotNull(connection, "connection");
        return fromConnection(connection,
                new Builder(connection.getConnectionType()));
    }

    /**
     * Creates a new {@code Connection} object from the specified JSON object.
     *
     * @param jsonObject a JSON object which provides the data for the Connection to be created.
     * @return a new Connection which is initialised with the extracted data from {@code jsonObject}.
     * @throws NullPointerException if {@code jsonObject} is {@code null}.
     * @throws org.eclipse.ditto.json.JsonParseException if {@code jsonObject} is not an appropriate JSON object.
     * @throws org.eclipse.ditto.json.JsonMissingFieldException if {@code jsonObject} does not contain a value at the defined location.
     */
    public static Connection fromJson(final JsonObject jsonObject) {
        final ConnectionType type = getConnectionTypeOrThrow(jsonObject);
        final Builder builder = new Builder(type);
        buildFromJson(getJsonObjectWithEmptyUri(jsonObject), builder);
        return builder.build();
    }

    private static JsonObject getJsonObjectWithEmptyUri(final JsonObject jsonObject) {
        if (!jsonObject.contains(JsonFields.URI.getPointer())) {
            return jsonObject.set(JsonFields.URI, "");
        }
        if (!jsonObject.getValue(Connection.JsonFields.URI).isPresent()) {
            return jsonObject.set(JsonFields.URI, "");
        }
        return jsonObject;
    }


    /**
     * Builder for {@code AbstractConnectionBuilder}.
     */
    @NotThreadSafe
    private static final class Builder extends AbstractConnectionBuilder {

        Builder(final ConnectionType connectionType) {
            super(connectionType);
        }

        @Override
        public Connection build() {
            System.out.println("In build of GooglePubSubConnection.Builder"); // TODO remove
//            super.checkSourceAndTargetAreValid(); TODO perhaps include
//            super.checkAuthorizationContextsAreValid(); TODO perhaps include back
//            super.checkConnectionAnnouncementsOnlySetIfClientCount1();  TODO perhaps include back
//            super.migrateLegacyConfigurationOnTheFly(); TODO perhaps include back
            return new GooglePubSubConnection(this);
        }

    }

}
