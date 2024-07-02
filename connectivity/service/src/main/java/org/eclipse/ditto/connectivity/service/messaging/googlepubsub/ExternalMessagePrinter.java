package org.eclipse.ditto.connectivity.service.messaging.googlepubsub;

import org.eclipse.ditto.connectivity.api.ExternalMessage;

public class ExternalMessagePrinter {

    public static void printExternalMessage(ExternalMessage message) {
        System.out.println("Headers:");
        message.getHeaders().forEach((key, value) -> System.out.println(key + ": " + value));

        System.out.println("\nContent-Type:");
        message.findContentType().ifPresent(contentType -> System.out.println(contentType));

        System.out.println("\nTopic Path:");
        message.getTopicPath().ifPresent(topicPath -> System.out.println(topicPath.toString()));

        System.out.println("\nAuthorization Context:");
        message.getAuthorizationContext().ifPresent(context -> System.out.println(context.toString()));

        System.out.println("\nEnforcement Filter:");
        message.getEnforcementFilter().ifPresent(filter -> System.out.println(filter.toString()));

        System.out.println("\nHeader Mapping:");
        message.getHeaderMapping().ifPresent(mapping -> System.out.println(mapping.toString()));

        System.out.println("\nPayload Mapping:");
        message.getPayloadMapping().ifPresent(mapping -> System.out.println(mapping.toString()));

        System.out.println("\nSource Address:");
        message.getSourceAddress().ifPresent(address -> System.out.println(address));

        System.out.println("\nSource:");
        message.getSource().ifPresent(source -> System.out.println(source.toString()));

        System.out.println("\nInternal Headers:");
        System.out.println(message.getInternalHeaders().toString());

        System.out.println("\nPayload Type:");
        System.out.println(message.getPayloadType().toString());

        System.out.println("\nText Payload:");
        message.getTextPayload().ifPresent(payload -> System.out.println(payload));

        System.out.println("\nByte Payload:");
        message.getBytePayload().ifPresent(payload -> System.out.println(payload.toString()));

        System.out.println("\nMessage Type:");
        if (message.isTextMessage()) {
            System.out.println("Text Message");
        } else if (message.isBytesMessage()) {
            System.out.println("Bytes Message");
        } else {
            System.out.println("Unknown Message Type");
        }

        System.out.println("\nResponse:");
        System.out.println(message.isResponse());

        System.out.println("\nError:");
        System.out.println(message.isError());
    }
}


