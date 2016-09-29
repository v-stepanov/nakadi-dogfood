package org.zalando.nakadi.dogfood.subscription;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.dogfood.subscription.domain.NakadiCursor;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;
import static java.text.MessageFormat.format;

public class Utils {

    static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    public static int commitCursors(final String baseUrl, final String subscriptionId, final List<NakadiCursor> cursors,
                                    final String streamId, final Optional<String> tokenOrNone) throws IOException {

        URL url = new URL(format("{0}/subscriptions/{1}/cursors", baseUrl, subscriptionId));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        tokenOrNone.ifPresent(token -> conn.setRequestProperty("Authorization", "Bearer " + token));
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("X-Nakadi-StreamId", streamId);

        String input = MAPPER.writeValueAsString(new CursorsWrapper(cursors));
        OutputStream os = conn.getOutputStream();
        os.write(input.getBytes());
        os.flush();

        final int responseCode = conn.getResponseCode();
        os.close();
        conn.disconnect();
        return responseCode;
    }

    @Immutable
    public static class CursorsWrapper {

        private final List<NakadiCursor> items;

        public CursorsWrapper(final List<NakadiCursor> items) {
            this.items = items;
        }

        public List<NakadiCursor> getItems() {
            return items;
        }
    }
}
