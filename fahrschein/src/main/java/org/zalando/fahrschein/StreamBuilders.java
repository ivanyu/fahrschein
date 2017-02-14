package org.zalando.fahrschein;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.zalando.fahrschein.domain.Lock;
import org.zalando.fahrschein.domain.Subscription;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

class StreamBuilders {
    abstract static class AbstractStreamBuilder implements StreamBuilder {
        protected final URI baseUri;
        protected final ClientHttpRequestFactory clientHttpRequestFactory;
        protected final CursorManager cursorManager;
        protected final ObjectMapper objectMapper;
        @Nullable
        protected final BackoffStrategy backoffStrategy;
        @Nullable
        protected final StreamParameters streamParameters;
        @Nullable
        protected final MetricsCollector metricsCollector;

        protected AbstractStreamBuilder(URI baseUri, ClientHttpRequestFactory clientHttpRequestFactory, CursorManager cursorManager, ObjectMapper objectMapper, @Nullable BackoffStrategy backoffStrategy, @Nullable StreamParameters streamParameters, @Nullable MetricsCollector metricsCollector) {
            this.baseUri = baseUri;
            this.clientHttpRequestFactory = clientHttpRequestFactory;
            this.cursorManager = cursorManager;
            this.backoffStrategy = backoffStrategy;
            this.objectMapper = objectMapper;
            this.streamParameters = streamParameters;
            this.metricsCollector = metricsCollector;
        }

        protected abstract URI getURI(String queryString);
        protected abstract Set<String> getEventNames();
        @Nullable
        protected abstract Subscription getSubscription();
        @Nullable
        protected abstract Lock getLock();

        @Override
        public final <T> void listen(Class<T> eventClass, Listener<T> listener) throws IOException {
            final StreamParameters streamParameters = this.streamParameters != null ? this.streamParameters : new StreamParameters();
            final String queryString = streamParameters.toQueryString();

            final URI uri = getURI(queryString);
            final Set<String> eventNames = getEventNames();
            final Subscription subscription = getSubscription();
            final Lock lock = getLock();

            final BackoffStrategy backoffStrategy = this.backoffStrategy != null ? this.backoffStrategy : new ExponentialBackoffStrategy();
            final ObjectMapper objectMapper = this.objectMapper != null ? this.objectMapper : DefaultObjectMapper.INSTANCE;
            final MetricsCollector metricsCollector = this.metricsCollector != null ? this.metricsCollector :NoMetricsCollector.NO_METRICS_COLLECTOR;

            final NakadiReader<T> nakadiReader = new NakadiReader<>(uri, clientHttpRequestFactory, backoffStrategy, cursorManager, objectMapper, eventNames, subscription, lock, eventClass, listener, metricsCollector);

            nakadiReader.run();
        }

    }

    static class SubscriptionStreamBuilderImpl extends AbstractStreamBuilder implements StreamBuilder.SubscriptionStreamBuilder {
        private final Subscription subscription;

        SubscriptionStreamBuilderImpl(URI baseUri, ClientHttpRequestFactory clientHttpRequestFactory, CursorManager cursorManager, Subscription subscription, ObjectMapper objectMapper) {
            this(baseUri, clientHttpRequestFactory, cursorManager, subscription, objectMapper, null, null, null);
        }

        private SubscriptionStreamBuilderImpl(URI baseUri, ClientHttpRequestFactory clientHttpRequestFactory, CursorManager cursorManager, Subscription subscription, ObjectMapper objectMapper, @Nullable BackoffStrategy backoffStrategy, @Nullable StreamParameters streamParameters, @Nullable MetricsCollector metricsCollector) {
            super(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector);
            this.subscription = subscription;
        }

        @Override
        protected URI getURI(String queryString) {
            return baseUri.resolve(String.format("/subscriptions/%s/events?%s", subscription.getId(), queryString));
        }

        @Override
        protected Set<String> getEventNames() {
            return subscription.getEventTypes();
        }

        @Override
        protected Subscription getSubscription() {
            return subscription;
        }

        @Override
        protected Lock getLock() {
            return null;
        }

        @Override
        public SubscriptionStreamBuilder withBackoffStrategy(BackoffStrategy backoffStrategy) {
            return new SubscriptionStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, subscription, objectMapper, backoffStrategy, streamParameters, metricsCollector);
        }

        @Override
        public SubscriptionStreamBuilder withMetricsCollector(MetricsCollector metricsCollector) {
            return new SubscriptionStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, subscription, objectMapper, backoffStrategy, streamParameters, metricsCollector);
        }

        @Override
        public SubscriptionStreamBuilder withStreamParameters(StreamParameters streamParameters) {
            return new SubscriptionStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, subscription, objectMapper, backoffStrategy, streamParameters, metricsCollector);
        }

        @Override
        public StreamBuilder withObjectMapper(ObjectMapper objectMapper) {
            return new SubscriptionStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, subscription, objectMapper, backoffStrategy, streamParameters, metricsCollector);
        }
    }

    static class LowLevelStreamBuilderImpl extends AbstractStreamBuilder implements StreamBuilder.LowLevelStreamBuilder {
        private final String eventName;
        private final Lock lock;

        LowLevelStreamBuilderImpl(URI baseUri, ClientHttpRequestFactory clientHttpRequestFactory, CursorManager cursorManager, ObjectMapper objectMapper, String eventName) {
            this(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, null, null, null, eventName, null);
        }

        private LowLevelStreamBuilderImpl(URI baseUri, ClientHttpRequestFactory clientHttpRequestFactory, CursorManager cursorManager, ObjectMapper objectMapper, @Nullable BackoffStrategy backoffStrategy, @Nullable StreamParameters streamParameters, @Nullable MetricsCollector metricsCollector, String eventName, @Nullable Lock lock) {
            super(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector);
            this.eventName = eventName;
            this.lock = lock;
        }

        @Override
        protected URI getURI(String queryString) {
            return baseUri.resolve(String.format("/event-types/%s/events?%s", eventName, queryString));
        }

        @Override
        protected Set<String> getEventNames() {
            return Collections.singleton(eventName);
        }

        @Override
        protected Subscription getSubscription() {
            return null;
        }

        @Override
        protected Lock getLock() {
            return lock;
        }

        @Override
        public LowLevelStreamBuilder withBackoffStrategy(BackoffStrategy backoffStrategy) {
            return new LowLevelStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector, eventName, lock);
        }

        @Override
        public LowLevelStreamBuilder withMetricsCollector(MetricsCollector metricsCollector) {
            return new LowLevelStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector, eventName, lock);
        }

        @Override
        public LowLevelStreamBuilder withStreamParameters(StreamParameters streamParameters) {
            return new LowLevelStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector, eventName, lock);
        }

        @Override
        public StreamBuilder withObjectMapper(ObjectMapper objectMapper) {
            return new LowLevelStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector, eventName, lock);
        }

        @Override
        public LowLevelStreamBuilder withLock(Lock lock) {
            return new LowLevelStreamBuilderImpl(baseUri, clientHttpRequestFactory, cursorManager, objectMapper, backoffStrategy, streamParameters, metricsCollector, eventName, lock);
        }
    }
}
