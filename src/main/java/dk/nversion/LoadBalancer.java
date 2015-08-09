package dk.nversion;

import java.lang.Integer;
import java.lang.Object;
import java.lang.System;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class LoadBalancer {
    // Configured
    private int retries = 0;
    private int endpointCount = 1;
    private int failureRateMaxFailures = 0;
    private TimeUnit failureRateTimeUnit;
    private long failureRateTime;
    private LoadBalancerPolicy policy = LoadBalancerPolicy.ROUND_ROBIN;

    private long suspensionTime = 0;
    private TimeUnit suspensionTimeUnit;

    private AtomicLongArray[] failureTimes;
    private AtomicLongArray suspensionTimes;

    private AtomicInteger indexGenerator = new AtomicInteger(0);
    private AtomicLongArray successCounters;
    private AtomicLongArray failureCounters;

    private Map<Object,AtomicInteger> retryCounters = new ConcurrentHashMap<>();

    public <T> CompletableFuture<T> wrap(Supplier<CompletableFuture<T>> function)   {
        CompletableFuture<T> result = new CompletableFuture<>();
        retryHelper((index, retryCount) -> function.get(), result);
        return result;
    }

    public <T> CompletableFuture<T> wrap(Function<Integer, CompletableFuture<T>> function)   {
        CompletableFuture<T> result = new CompletableFuture<>();
        retryHelper((index, retryCount) -> function.apply(index), result);
        return result;
    }

    public <T> CompletableFuture<T> wrap(BiFunction<Integer, Integer, CompletableFuture<T>> function)   {
        CompletableFuture<T> result = new CompletableFuture<>();
        retryHelper(function, result);
        return result;
    }

    private <T> void retryHelper (BiFunction<Integer, Integer, CompletableFuture<T>> function, CompletableFuture<T> result) {
        final int retries = doRetry(function);
        final int index = getNextIndex(function);

        if(index > -1) {
            function.apply(index, retries).whenComplete((obj, ex) -> {
                if (ex == null) {
                    registerSuccess(function, index);
                    result.complete(obj);

                } else {
                    if (retries > 0) {
                        registerFailure(function, index, false);
                        retryHelper(function, result);

                    } else {
                        registerFailure(function, index, true);
                        result.completeExceptionally(ex);
                    }
                }
            });

        } else {
            result.completeExceptionally(new LoadBalancerException("All backends suspended"));
        }
    }

    private int doRetry(Object caller) {
        if(retryCounters.containsKey(caller)) {
            return retryCounters.get(caller).decrementAndGet();

        } else {
            retryCounters.put(caller, new AtomicInteger(this.retries));
            return this.retries;
        }
    }

    private int getNextIndex(Object caller) {
        long now = System.currentTimeMillis();

        // Try all endpoints if some are suspended
        int count = endpointCount;
        int[] tried = new int[endpointCount];
        while(count > 0) {
            // Increment index and convert negative values to positive if need be
            int index = indexGenerator.getAndIncrement() % endpointCount;
            if(index < 0) {
                index += endpointCount;
            }

            // Don't retry same index
            if(tried[index] == 0) {
                // Return index if endpoint is not suspended
                if (suspensionTime == 0 || suspensionTimes.get(index) < now) {
                    return index;
                }

                tried[index] = 1;
                count--;
            }
        }
        return -1;
    }

    private void registerSuccess(Object caller, int index) {
        successCounters.incrementAndGet(index);
        retryCounters.remove(caller);
    }

    private void registerFailure(Object caller, int index, boolean last) {
        failureCounters.incrementAndGet(index);

        // Try to save the failure time in the failureTimes array
        long now = System.currentTimeMillis();
        long oldestFailureTime =  now - failureRateTimeUnit.toMillis(failureRateTime);
        int i;
        for(i = 0; i < failureTimes[index].length(); i++) {
            if(failureTimes[index].get(i) < oldestFailureTime) {
                failureTimes[index].set(i, now);
                break;
            }
        }
        // If all failureTimes slots are used then we can suspend the endpoint
        if(suspensionTime != 0 && i == failureTimes[index].length() - 1) {
            suspensionTimes.set(index, now + suspensionTimeUnit.toMillis(suspensionTime));
        }

        // Remove the caller
        if(last) {
            retryCounters.remove(caller);
        }
    }

    public static CompletableFutureConfigBuilder builder() {
        return  new CompletableFutureConfigBuilder();
    }

    public static final class CompletableFutureConfigBuilder {
        private LoadBalancer loadBalancer = new LoadBalancer();

        public CompletableFutureConfigBuilder setRetryCount(int retries) {
            loadBalancer.retries = retries;
            return this;
        }

        public CompletableFutureConfigBuilder setEndpointCount(int count) {
            loadBalancer.endpointCount = count;
            return this;
        }

        public CompletableFutureConfigBuilder setMaxFailureRate(int maxFailures, long time, TimeUnit timeUnit) {
            loadBalancer.failureRateMaxFailures = maxFailures;
            loadBalancer.failureRateTimeUnit = timeUnit;
            loadBalancer.failureRateTime = time;
            return this;
        }

        public CompletableFutureConfigBuilder setSuspensionTime(long time, TimeUnit timeUnit) {
            loadBalancer.suspensionTime = time;
            loadBalancer.suspensionTimeUnit = timeUnit;
            return this;
        }

        public CompletableFutureConfigBuilder setPolicy(LoadBalancerPolicy policy) {
            loadBalancer.policy = policy;
            return this;
        }

        public LoadBalancer build() {
            loadBalancer.suspensionTimes = new AtomicLongArray(loadBalancer.endpointCount);
            loadBalancer.successCounters = new AtomicLongArray(loadBalancer.endpointCount);
            loadBalancer.failureCounters = new AtomicLongArray(loadBalancer.endpointCount);

            loadBalancer.failureTimes = new AtomicLongArray[loadBalancer.endpointCount];
            for(int i = 0; i < loadBalancer.failureTimes.length; i++) {
                loadBalancer.failureTimes[i] = new AtomicLongArray(loadBalancer.failureRateMaxFailures);
            }
            return loadBalancer;
        }
    }
}
