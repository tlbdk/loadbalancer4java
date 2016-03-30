package dk.nversion;

import java.lang.Integer;
import java.lang.Object;
import java.lang.System;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
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

    ScheduledExecutorService scheduledExecutorService;
    private long monitorCheckInterval;
    private TimeUnit monitorCheckTimeUnit;
    private int monitorUnhealthyThreshold;
    private int monitorHealthyThreshold;
    private AtomicIntegerArray monitorUnhealthyCounters;
    private AtomicIntegerArray monitorHealthyCounters;
    private Function<Integer, CompletableFuture<Boolean>> monitorFunction;

    private AtomicLongArray latencyTimes;

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
        final long start = System.currentTimeMillis();
        if(index > -1) {
            try {
                function.apply(index, retries).whenComplete((obj, ex) -> {
                    if (ex == null) {
                        registerSuccess(function, index, start);
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

            } catch (Exception e) {
                if (retries > 0) {
                    registerFailure(function, index, false);
                    retryHelper(function, result);

                } else {
                    registerFailure(function, index, true);
                    result.completeExceptionally(e);
                }
            }

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

        if(this.policy == LoadBalancerPolicy.ROUND_ROBIN) {
            // Try all endpoints if some are suspended
            int count = endpointCount;
            int[] tried = new int[endpointCount];
            while (count > 0) {
                // Increment index and convert negative values to positive if need be
                int index = indexGenerator.getAndIncrement() % endpointCount;
                if (index < 0) {
                    index += endpointCount;
                }

                // Don't retry same index
                if (tried[index] == 0) {
                    // Return index if endpoint is not suspended
                    if (suspensionTimes.get(index) < now) {
                        return index;
                    }

                    tried[index] = 1;
                    count--;
                }
            }

        } else if(this.policy == LoadBalancerPolicy.LATENCY_LAST) {
            int index = -1;
            for(int i = 0;  i < latencyTimes.length(); i++) {
                long current = latencyTimes.get(i);
                long smallest = Long.MAX_VALUE;
                if(current < smallest && suspensionTimes.get(i) < now) {
                    smallest = current;
                    index = i;
                }
            }
            return index;
        }

        return -1;
    }

    private void registerSuccess(Object caller, int index, long start) {
        successCounters.incrementAndGet(index);
        retryCounters.remove(caller);
        if(policy == LoadBalancerPolicy.LATENCY_LAST) {
            latencyTimes.set(index, System.currentTimeMillis() - start);
        }
    }

    private void registerFailure(Object caller, int index, boolean last) {
        failureCounters.incrementAndGet(index);

        if(policy == LoadBalancerPolicy.LATENCY_LAST) {
            // Make sure we don't pick a backend with a failed request next time
            latencyTimes.set(index, Long.MAX_VALUE - 1);
        }

        if(suspensionTime > 0) {
            // Try to save the failure time in the failureTimes array
            long now = System.currentTimeMillis();
            long oldestFailureTime = now - failureRateTimeUnit.toMillis(failureRateTime);
            int i;
            for (i = 0; i < failureTimes[index].length(); i++) {
                if (failureTimes[index].get(i) < oldestFailureTime) {
                    failureTimes[index].set(i, now);
                    break;
                }
            }
            // If all failureTimes slots are used then we can suspend the endpoint
            if (failureTimes[index].length() == 0 || i == failureTimes[index].length() - 1) {
                suspensionTimes.set(index, now + suspensionTimeUnit.toMillis(suspensionTime));
            }
        }

        // Remove the caller
        if(last) {
            retryCounters.remove(caller);
        }
    }

    private void checkMonitors() {
        for (int i = 0; i < endpointCount; i++) {
            final int index = i;
            try {
                monitorFunction.apply(index).whenComplete((result, ex) -> {
                    if (result && ex == null) {
                        // Unset suspension time when we hit the healthy threshold
                        if(monitorHealthyCounters.incrementAndGet(index) >= monitorHealthyThreshold) {
                            suspensionTimes.set(index, 0);
                            monitorHealthyCounters.set(index, 0);
                        }
                        // Reset unhealthy counter
                        if(monitorUnhealthyCounters.get(index) > 0) {
                            monitorUnhealthyCounters.set(index, 0);
                        }

                    } else {
                        // Set suspension time when we hit the unhealthy threshold
                        if(monitorUnhealthyCounters.incrementAndGet(index) >= monitorUnhealthyThreshold) {
                            suspensionTimes.set(index, Long.MAX_VALUE);
                            monitorUnhealthyCounters.set(index, 0);
                        }
                        // Reset healthy counter
                        if(monitorHealthyCounters.get(index) > 0) {
                            monitorHealthyCounters.set(index, 0);
                        }
                    }
                });

            } catch (Exception ex) { // Got exception trying to create the future
                // Set suspension time when we hit the unhealthy threshold
                if(monitorUnhealthyCounters.incrementAndGet(index) >= monitorUnhealthyThreshold) {
                    suspensionTimes.set(index, Long.MAX_VALUE);
                    monitorUnhealthyCounters.set(index, 0);
                }
                // Reset healthy counter
                if(monitorHealthyCounters.get(index) > 0) {
                    monitorHealthyCounters.set(index, 0);
                }
            }
        }
    }

    public static LoadBalancerBuilder builder() {
        return  new LoadBalancerBuilder();
    }

    public static final class LoadBalancerBuilder {
        private LoadBalancer loadBalancer = new LoadBalancer();

        public LoadBalancerBuilder setRetryCount(int retries) {
            loadBalancer.retries = retries;
            return this;
        }

        public LoadBalancerBuilder setEndpointCount(int count) {
            loadBalancer.endpointCount = count;
            return this;
        }

        public LoadBalancerBuilder setMaxFailureRate(int maxFailures, long failuresTime, TimeUnit failuresTimeUnit, long suspensionTime, TimeUnit suspensionTimeUnit) {
            loadBalancer.failureRateMaxFailures = maxFailures;
            loadBalancer.failureRateTime = failuresTime;
            loadBalancer.failureRateTimeUnit = failuresTimeUnit;
            loadBalancer.suspensionTime = suspensionTime;
            loadBalancer.suspensionTimeUnit = suspensionTimeUnit;
            return this;
        }

        public LoadBalancerBuilder setPolicy(LoadBalancerPolicy policy) {
            loadBalancer.policy = policy;
            return this;
        }

        public LoadBalancerBuilder setMonitor(long checkInterval, TimeUnit checkTimeUnit,  int unhealthyThreshold, int healthyThreshold, Function<Integer, CompletableFuture<Boolean>> function) {
            loadBalancer.monitorCheckInterval = checkInterval;
            loadBalancer.monitorCheckTimeUnit = checkTimeUnit;
            loadBalancer.monitorUnhealthyThreshold = unhealthyThreshold;
            loadBalancer.monitorHealthyThreshold = healthyThreshold;
            loadBalancer.monitorFunction = function;
            return this;
        }

        public <K, V> LoadBalancerBuilder setCache(CompletableFutureCache<K, V> cache) {
            //ConcurrentHashMap<T,T> map = ;
            return this;
        }

        public <T> LoadBalancerBuilder setCache(long checkInterval, TimeUnit checkTimeUnit, Class<T> returnType, BiFunction<Integer, CompletableFutureCache, CompletableFuture<T>> function) {
            loadBalancer.monitorCheckInterval = checkInterval;
            loadBalancer.monitorCheckTimeUnit = checkTimeUnit;
            //loadBalancer.monitorFunction = function;
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

            // Setup monitor if it has been set
            if(loadBalancer.monitorFunction != null) {
                loadBalancer.monitorUnhealthyCounters = new AtomicIntegerArray(loadBalancer.endpointCount);
                loadBalancer.monitorHealthyCounters = new AtomicIntegerArray(loadBalancer.endpointCount);
                loadBalancer.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                loadBalancer.scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    loadBalancer.checkMonitors();
                }, 0, loadBalancer.monitorCheckInterval, loadBalancer.monitorCheckTimeUnit);
            }
            return loadBalancer;
        }
    }
}

