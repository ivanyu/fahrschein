package org.zalando.fahrschein;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamParameters {
    @Nullable
    private final Integer batchLimit;
    @Nullable
    private final Integer streamLimit;
    @Nullable
    private final Integer batchFlushTimeout;
    @Nullable
    private final Integer streamTimeout;
    @Nullable
    private final Integer streamKeepAliveLimit;
    // Only used in the subscription api
    @Nullable
    private final Integer maxUncommittedEvents;

    private StreamParameters(@Nullable Integer batchLimit, @Nullable Integer streamLimit, @Nullable Integer batchFlushTimeout, @Nullable Integer streamTimeout, @Nullable Integer streamKeepAliveLimit, @Nullable Integer maxUncommittedEvents) {
        this.batchLimit = batchLimit;
        this.streamLimit = streamLimit;
        this.batchFlushTimeout = batchFlushTimeout;
        this.streamTimeout = streamTimeout;
        this.streamKeepAliveLimit = streamKeepAliveLimit;
        this.maxUncommittedEvents = maxUncommittedEvents;
    }

    public StreamParameters() {
        this(null, null, null, null, null, null);
    }

    String toQueryString() {
        final List<String> params = new ArrayList<>(6);

        if (batchLimit != null) {
            params.add("batch_limit=" + batchLimit);
        }
        if (streamLimit != null) {
            params.add("stream_limit=" + streamLimit);
        }
        if (batchFlushTimeout != null) {
            params.add("batch_flush_timeout=" + batchFlushTimeout);
        }
        if (streamTimeout != null) {
            params.add("stream_timeout=" + streamTimeout);
        }
        if (streamKeepAliveLimit != null) {
            params.add("stream_keep_alive_limit=" + streamKeepAliveLimit);
        }
        if (maxUncommittedEvents != null) {
            params.add("max_uncommitted_events=" + maxUncommittedEvents);
        }

        final StringBuilder sb = new StringBuilder();
        final Iterator<String> iterator = params.iterator();
        while (iterator.hasNext()) {
            String param = iterator.next();
            sb.append(param);
            if (iterator.hasNext()) {
                sb.append("&");
            }
        }
        return sb.toString();
    }

    public StreamParameters withBatchLimit(int batchLimit) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    public StreamParameters withStreamLimit(int streamLimit) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    public StreamParameters withBatchFlushTimeout(int batchFlushTimeout) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    public StreamParameters withStreamTimeout(int streamTimeout) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    public StreamParameters withStreamKeepAliveLimit(int streamKeepAliveLimit) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    public StreamParameters withMaxUncommittedEvents(int maxUncommittedEvents) {
        return new StreamParameters(batchLimit, streamLimit, batchFlushTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents);
    }

    @Nullable
    public Integer getBatchLimit() {
        return batchLimit;
    }

    @Nullable
    public Integer getStreamLimit() {
        return streamLimit;
    }

    @Nullable
    public Integer getBatchFlushTimeout() {
        return batchFlushTimeout;
    }

    @Nullable
    public Integer getStreamTimeout() {
        return streamTimeout;
    }

    @Nullable
    public Integer getStreamKeepAliveLimit() {
        return streamKeepAliveLimit;
    }

    @Nullable
    public Integer getMaxUncommittedEvents() {
        return maxUncommittedEvents;
    }
}
