package org.zalando.nakadi.dogfood;

import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.dogfood.subscription.NakadiReader;
import org.zalando.nakadi.dogfood.subscription.Utils;
import org.zalando.nakadi.dogfood.subscription.domain.NakadiBatch;
import org.zalando.nakadi.dogfood.subscription.domain.NakadiCursor;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;


public class DogFood {

    private static final String TOKEN = "---";
    private static final String NAKADI_URL = "https://example.com";
    private static final String SUBSCRIPTION_ID = "---";

    public static void main(String[] args) throws InterruptedException, IOException {

        final NakadiReader client = new NakadiReader(NAKADI_URL, SUBSCRIPTION_ID, "", Optional.of(TOKEN));
        client.start();

        while (true) {
            final Optional<NakadiBatch> batchOrNone = client.takeNextBatch();
            if (batchOrNone.isPresent()) {
                final NakadiBatch batch = batchOrNone.get();
                final NakadiCursor cursor = batch.getCursor();
                final int commitResult = Utils.commitCursors(NAKADI_URL, SUBSCRIPTION_ID, ImmutableList.of(cursor),
                        client.getSessionId(), Optional.of(TOKEN));
                System.out.println(new Date() + " | commit result: " + commitResult);
            } else {
                Thread.sleep(100);
            }
        }
    }


}