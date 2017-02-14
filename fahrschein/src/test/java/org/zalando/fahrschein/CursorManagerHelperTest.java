package org.zalando.fahrschein;

import org.junit.Test;
import org.zalando.fahrschein.domain.Cursor;
import org.zalando.fahrschein.domain.Partition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CursorManagerHelperTest {

    private final CursorManager cursorManager = mock(CursorManager.class);

    private void run(@Nullable String initialOffset, String oldestAvailableOffset, String newestAvailableOffset, @Nullable String expectedOffset) throws IOException {
        when(cursorManager.getCursors("test")).thenReturn(initialOffset == null ? Collections.<Cursor>emptyList() : singletonList(new Cursor("0", initialOffset)));
        CursorManagerHelper.updatePartitions(cursorManager, "test", singletonList(new Partition("0", oldestAvailableOffset, newestAvailableOffset)));
        if (expectedOffset != null) {
            verify(cursorManager).onSuccess(eq("test"), refEq(new Cursor("0", expectedOffset)));
        } else {
            verify(cursorManager, never()).onSuccess(anyString(), any(Cursor.class));
        }
    }

    @Test
    public void shouldNotUpdatePartitionWhenOffsetStillAvailable() throws IOException {
        run("20", "10", "30", null);
    }

    @Test
    public void shouldNotUpdatePartitionWhenOffsetStillAvailableAndMore() throws IOException {
        run("234", "12", "2345", null);
    }

    @Test
    public void shouldUpdatePartitionWhenNoCursorAndLastConsumedOffsetNoLongerAvailable() throws IOException {
        run(null, "10", "20", "BEGIN");
    }

    @Test
    public void shouldUpdatePartitionWhenLastConsumedOffsetNoLongerAvailable() throws IOException {
        run("5", "10", "20", "BEGIN");
    }

    @Test
    public void shouldUpdatePartitionToBeginWhenNoCursorAndPartitionIsEmpty() throws IOException {
        run(null, "0", "BEGIN", "BEGIN");
    }

    @Test
    public void shouldNotUpdatePartitionWhenCursorIsAreadyAtBegin() throws IOException {
        run("BEGIN", "0", "BEGIN", null);
    }

    @Test
    public void shouldUpdatePartitionToNewestAvailableWhenNoCursorAndPartitionIsExpired() throws IOException {
        run(null, "2", "1", "BEGIN");
    }

    @Test
    public void shouldUpdatePartitionToNewestAvailableWhenPartitionIsExpired() throws IOException {
        run("10", "22", "21", "BEGIN");
    }

    @Test
    public void shouldUpdatePartitionToNewestAvailableWhenPartitionIsExpiredLongAgo() throws IOException {
        run("10", "1234", "2345", "BEGIN");
    }

}
