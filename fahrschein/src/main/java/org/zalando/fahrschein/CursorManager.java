package org.zalando.fahrschein;

import org.zalando.fahrschein.domain.Cursor;
import org.zalando.fahrschein.domain.Subscription;

import java.io.IOException;
import java.util.Collection;

/**
 * Manages cursor offsets for one consumer. One consumer can handle several distinct events.
 */
public interface CursorManager {

    void onSuccess(String eventName, Cursor cursor) throws IOException;

    void onError(String eventName, Cursor cursor, Throwable throwable) throws IOException;

    Collection<Cursor> getCursors(String eventName) throws IOException;

    void addSubscription(Subscription subscription);

    void addStreamId(Subscription subscription, String streamId);


}
