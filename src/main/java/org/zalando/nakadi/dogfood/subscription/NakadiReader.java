package org.zalando.nakadi.dogfood.subscription;

import org.zalando.nakadi.dogfood.subscription.domain.NakadiBatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.text.MessageFormat.format;
import static org.zalando.nakadi.dogfood.subscription.Utils.MAPPER;

public class NakadiReader implements Runnable {

    public static final String SESSION_ID_UNKNOWN = "UNKNOWN";

    private final String baseUrl;
    private final String subscriptionId;
    private final String params;
    private Optional<String> token;

    private volatile String sessionId;
    private volatile boolean running;
    private InputStream inputStream;
    private Queue<NakadiBatch> queue;


    public NakadiReader(final String baseUrl, final String subscriptionId, final String params,
                        final Optional<String> token) {
        this.baseUrl = baseUrl;
        this.subscriptionId = subscriptionId;
        this.params = params;
        this.running = false;
        this.sessionId = SESSION_ID_UNKNOWN;
        this.token = token;
        this.queue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {
        try {
            final String url = format("{0}/subscriptions/{1}/events?{2}", baseUrl, subscriptionId, params);
            final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            token.ifPresent(token -> conn.setRequestProperty("Authorization", "Bearer " + token));
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new IOException("Response code is " + conn.getResponseCode());
            }
            sessionId = conn.getHeaderField("X-Nakadi-StreamId");
            inputStream = conn.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            running = true;

            try {
                while (true) {
                    final String line = reader.readLine();
                    if (line != null) {
                        System.out.println(new Date() + " > Got batch: " + line);
                        final NakadiBatch batch = MAPPER.readValue(line, NakadiBatch.class);
                        queue.add(batch);
                    } else {
                        break;
                    }
                }
            } finally {
                try {
                    inputStream.close();
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            running = false;
        }
    }

    public NakadiReader start() {
        if (!running) {
            queue.clear();
            final Thread thread = new Thread(this);
            thread.start();
            return this;
        } else {
            throw new IllegalStateException("Client has not yet finished with previous run");
        }
    }

    public boolean close() {
        if (running) {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                running = false;
            }
            return true;
        } else {
            return false;
        }
    }

    public Optional<NakadiBatch> takeNextBatch() {
        return Optional.ofNullable(queue.poll());
    }

    public boolean isRunning() {
        return running;
    }

    public String getSessionId() {
        return sessionId;
    }
}
