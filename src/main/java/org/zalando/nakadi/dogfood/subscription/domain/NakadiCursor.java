package org.zalando.nakadi.dogfood.subscription.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
public class NakadiCursor {

    private final String eventType;

    private final String cursorToken;

    private final String partition;

    private final String offset;

    public NakadiCursor(@JsonProperty("partition") final String partition,
                        @JsonProperty("offset") final String offset,
                        @JsonProperty("event_type") final String eventType,
                        @JsonProperty("cursor_token") final String cursorToken) {
        this.partition = partition;
        this.offset = offset;
        this.eventType = eventType;
        this.cursorToken = cursorToken;
    }

    public String getPartition() {
        return partition;
    }

    public String getOffset() {
        return offset;
    }

    public String getEventType() {
        return eventType;
    }

    public String getCursorToken() {
        return cursorToken;
    }

    @Override
    public String toString() {
        return "NakadiCursor{" +
                "partition='" + getPartition() + '\'' +
                ", offset='" + getOffset() + '\'' +
                ", eventType='" + eventType + '\'' +
                ", cursorToken='" + cursorToken + '\'' +
                '}';
    }
}