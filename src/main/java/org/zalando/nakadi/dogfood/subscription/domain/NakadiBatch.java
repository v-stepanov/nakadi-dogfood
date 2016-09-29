package org.zalando.nakadi.dogfood.subscription.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;

@Immutable
public class NakadiBatch {

    private final NakadiCursor cursor;

    private final List<Map> events;

    private final Metadata info;

    public NakadiBatch(@JsonProperty("cursor") final NakadiCursor cursor,
                       @Nullable @JsonProperty("events") final List<Map> events,
                       @Nullable @JsonProperty("info") final Metadata info) {
        this.cursor = cursor;
        this.events = Optional.ofNullable(events).orElse(ImmutableList.of());
        this.info = info;
    }

    public NakadiCursor getCursor() {
        return cursor;
    }

    public List<Map> getEvents() {
        return unmodifiableList(events);
    }

    public Metadata getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "StreamBatch{" +
                "cursor=" + cursor +
                ", events=" + events +
                ", metadata=" + info +
                '}';
    }


    @Immutable
    public static class Metadata {

        private final String debug;

        public Metadata(@Nullable @JsonProperty("debug") final String debug) {
            this.debug = debug;
        }

        @Nullable
        public String getDebug() {
            return debug;
        }

        @Override
        public String toString() {
            return "debug=" + debug;
        }
    }
}
