package com.rtm516.mcxboxbroadcast.core;

import com.google.gson.JsonParseException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MessageListener {
    private final FriendManager friendManager;
    private final SessionManagerCore sessionManager;
    private final HttpClient httpClient;
    private final Logger logger;
    private final int pollInterval;
    private Instant lastProcessedTimestamp = Instant.MIN;

    public MessageListener(FriendManager friendManager, SessionManagerCore sessionManager, 
                          HttpClient httpClient, Logger logger, int pollInterval) {
        this.friendManager = friendManager;
        this.sessionManager = sessionManager;
        this.httpClient = httpClient;
        this.logger = logger;
        this.pollInterval = pollInterval;
    }

    public void start() {
        sessionManager.scheduledThread().scheduleWithFixedDelay(this::checkMessages, 0, pollInterval, TimeUnit.SECONDS);
    }

    private void checkMessages() {
        try {
            String xuid = sessionManager.getXuid();
            if (xuid == null || xuid.isEmpty()) {
                logger.error("XUID is not available. Cannot fetch messages.");
                return;
            }

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format(Constants.MESSAGES_INBOX, xuid)))
                .header("Authorization", sessionManager.getTokenHeader())
                .header("x-xbl-contract-version", "2")
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200) {
                XboxMessageResponse messages = Constants.GSON.fromJson(response.body(), XboxMessageResponse.class);
                processNewMessages(messages.messages); // Process only new messages
            } else if (response.statusCode() == 429) {
                handleRateLimit(response);
            } else if (response.statusCode() == 403) {
                logger.error("Missing permissions to fetch messages. Ensure the XSTS token includes 'message_restricted' scope.");
            } else {
                logger.debug("Failed to fetch messages: " + response.body());
            }
        } catch (IOException | InterruptedException | JsonParseException e) {
            logger.error("Error checking messages: " + e.getMessage());
        }
    }

    private void handleRateLimit(HttpResponse<String> response) {
        Optional<String> retryAfter = response.headers().firstValue("Retry-After");
        if (retryAfter.isPresent()) {
            try {
                int delay = Integer.parseInt(retryAfter.get());
                logger.warn("Message API rate limited - retrying in " + delay + " seconds");
                sessionManager.scheduledThread().schedule(this::checkMessages, delay, TimeUnit.SECONDS);
            } catch (NumberFormatException e) {
                logger.error("Invalid Retry-After header: " + retryAfter.get());
            }
        } else {
            logger.warn("Rate limit encountered, but no Retry-After header found.");
        }
    }

    private void processNewMessages(List<XboxMessage> messages) {
        // Filter messages newer than the last processed timestamp
        List<XboxMessage> newMessages = messages.stream()
            .filter(message -> message.timestamp.isAfter(lastProcessedTimestamp))
            .collect(Collectors.toList());

        if (newMessages.isEmpty()) {
            logger.debug("No new messages to process.");
            return;
        }

        // Group messages by sender and keep only the most recent one per sender
        Map<String, XboxMessage> latestMessagesBySender = new HashMap<>();
        for (XboxMessage message : newMessages) {
            latestMessagesBySender.merge(
                message.senderXuid,
                message,
                (existing, current) -> current.timestamp.isAfter(existing.timestamp) ? current : existing
            );
        }

        // Process the most recent message from each sender
        for (XboxMessage message : latestMessagesBySender.values()) {
            logger.debug("Processing message from " + message.senderXuid + ": " + message.content);
            friendManager.processMessage(message.content, message.senderXuid);
        }

        // Update the last processed timestamp
        lastProcessedTimestamp = newMessages.stream()
            .map(message -> message.timestamp)
            .max(Instant::compareTo)
            .orElse(lastProcessedTimestamp);

        logger.debug("Updated last processed timestamp to: " + lastProcessedTimestamp);
    }

    private static class XboxMessageResponse {
        public List<XboxMessage> messages;
    }

    private static class XboxMessage {
        public String content;
        public String senderXuid;
        public Instant timestamp;
    }
}
