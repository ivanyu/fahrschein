package org.zalando.fahrschein;

import org.zalando.fahrschein.domain.Cursor;
import org.zalando.fahrschein.domain.Partition;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CursorManagerHelper {
    /**
     * Initializes offsets to start streaming from the newest available offset.
     */
    public static void fromNewestAvailableOffsets(CursorManager cursorManager, String eventName, List<Partition> partitions) throws IOException {
        for (Partition partition : partitions) {
            cursorManager.onSuccess(eventName, new Cursor(partition.getPartition(), partition.getNewestAvailableOffset()));
        }
    }

    /**
     * Initializes offsets to start streaming at the oldest available offset (BEGIN).
     */
    public static void fromOldestAvailableOffset(CursorManager cursorManager, String eventName, List<Partition> partitions) throws IOException {
        for (Partition partition : partitions) {
            cursorManager.onSuccess(eventName, new Cursor(partition.getPartition(), "BEGIN"));
        }
    }

    /**
     * Updates offsets in case the currently stored offset is no longer available. Streaming will start at the oldest available offset (BEGIN) to minimize the amount of events skipped.
     */
    public static void updatePartitions(CursorManager cursorManager, String eventName, List<Partition> partitions) throws IOException {

        final Collection<Cursor> cursors = cursorManager.getCursors(eventName);
        final Map<String, Cursor> cursorsByPartition = new HashMap<>();
        for (Cursor cursor : cursors) {
            cursorsByPartition.put(cursor.getPartition(), cursor);
        }

        for (Partition partition : partitions) {
            final Cursor cursor = cursorsByPartition.get(partition.getPartition());
            if (cursor == null || (!"BEGIN".equals(cursor.getOffset()) && OffsetComparator.INSTANCE.compare(cursor.getOffset(), partition.getOldestAvailableOffset()) < 0)) {
                cursorManager.onSuccess(eventName, new Cursor(partition.getPartition(), "BEGIN"));
            }
        }
    }

}
