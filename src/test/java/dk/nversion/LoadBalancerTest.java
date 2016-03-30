package dk.nversion;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LoadBalancerTest {

    @Test
    public void testLoadBalancedSimple() throws Exception {
        LoadBalancer config = LoadBalancer.builder().build();
        final CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<String> future = config.wrap(() -> simpleSuccess("url"));
        Assert.assertEquals(future.get(), "url");
    }

    private CompletableFuture<String> simpleSuccess(String url) {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        completableFuture.complete(url);
        return completableFuture;
    }


    @Test
    public void testLoadBalancedRetry() throws Exception {
        String[] urls = new String[] { "url1", "url2", "url3" };

        LoadBalancer config = LoadBalancer.builder()
                .setMaxFailureRate(10, 30, TimeUnit.SECONDS, 60, TimeUnit.SECONDS)
                .setRetryCount(2)
                .setEndpointCount(urls.length)
                .setPolicy(LoadBalancerPolicy.ROUND_ROBIN)
                .build();

        final CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<String> retryFuture = config.wrap((index, retryCount) -> {
            CompletableFuture<String> completableFuture = new CompletableFuture<>();
            if (retryCount > 0) {
                completableFuture.completeExceptionally(new Exception("Stuff"));

            } else {
                completableFuture.complete(urls[index]);
            }
            return completableFuture;
        });

        retryFuture.whenComplete((str, ex) -> {
            // TODO: Move outside to handle the exception
            Assert.assertEquals(str, "url3");
            Assert.assertNull(ex);
            latch.countDown();
        });
        if(!latch.await(1000, TimeUnit.MILLISECONDS)) {
            Assert.fail("Got timeout waiting for future to complete");
        }
    }

    @Test
    public void testLoadBalancedRoundRobin() throws Exception {
        String[] urls = new String[] { "url1", "url2", "url3" };

        LoadBalancer config = LoadBalancer.builder()
                .setMaxFailureRate(10, 30, TimeUnit.SECONDS, 60, TimeUnit.SECONDS)
                .setRetryCount(0)
                .setEndpointCount(urls.length)
                .setPolicy(LoadBalancerPolicy.ROUND_ROBIN)
                .build();

        List<CompletableFuture<String>> futureResults = new ArrayList<>();
        for(int i = 0; i < urls.length; i++) {
            CompletableFuture<String> roundRobinFuture = config.wrap((index) -> {
                CompletableFuture<String> completableFuture = new CompletableFuture<>();
                completableFuture.complete(urls[index]);
                return completableFuture;
            });
            futureResults.add(roundRobinFuture);
        }

        List<String> results = new ArrayList<>();
        for(CompletableFuture<String> future : futureResults) {
            results.add(future.get());
        }

        Assert.assertArrayEquals(urls, results.toArray(new String[results.size()]));
    }

    @Test
    public void testLoadBalancedSuspension() throws Exception {
        String[] urls = new String[] { "url1" };

        LoadBalancer config = LoadBalancer.builder()
                .setMaxFailureRate(1, 30, TimeUnit.SECONDS, 1000, TimeUnit.SECONDS)
                .setRetryCount(0)
                .setEndpointCount(urls.length)
                .setPolicy(LoadBalancerPolicy.ROUND_ROBIN)
                .build();

        List<CompletableFuture<String>> futureResults = new ArrayList<>();
        for(int i = 0; i < 3; i++) {
            CompletableFuture<String> roundRobinFuture = config.wrap(() -> {
                CompletableFuture<String> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(new Exception("Normal exception"));
                return completableFuture;
            });
            futureResults.add(roundRobinFuture);
        }

        List<Exception> results = new ArrayList<>();
        for(CompletableFuture<String> future : futureResults) {
            try {
                future.get();

            } catch (Exception ex){
                results.add(ex);
            }
        }

        Assert.assertEquals("java.lang.Exception: Normal exception", results.get(0).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(1).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(2).getMessage());
    }

    @Test
    public void testLoadBalancerMonitor() throws Exception {
        String[] urls = new String[] { "url1" };

        LoadBalancer config = LoadBalancer.builder()
                .setEndpointCount(urls.length)
                .setMonitor(100, TimeUnit.MILLISECONDS, 1, 1, (index) -> {
                    CompletableFuture<Boolean> monitorCheck = new CompletableFuture<Boolean>();
                    // Check urls[index] is working as expected
                    monitorCheck.complete(false);
                    return monitorCheck;
                })
                .build();

        Thread.sleep(500);
        List<CompletableFuture<String>> futureResults = new ArrayList<>();
        for(int i = 0; i < 3; i++) {
            CompletableFuture<String> roundRobinFuture = config.wrap(() -> {
                CompletableFuture<String> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(new Exception("Normal exception"));
                return completableFuture;
            });
            futureResults.add(roundRobinFuture);
        }

        List<Exception> results = new ArrayList<>();
        for(CompletableFuture<String> future : futureResults) {
            try {
                future.get();

            } catch (Exception ex){
                results.add(ex);
            }
        }

        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(0).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(1).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(2).getMessage());
    }


    /* @Test // TODO: Implement test for latency based load balancing
    public void testLoadBalancerLatency() throws Exception {
        String[] urls = new String[] { "url1" };

        LoadBalancer config = LoadBalancer.builder()
                .setEndpointCount(urls.length)
                .setPolicy(LoadBalancerPolicy.LATENCY_LAST)
                .build();

        Thread.sleep(500);
        List<CompletableFuture<String>> futureResults = new ArrayList<>();
        for(int i = 0; i < 3; i++) {
            CompletableFuture<String> roundRobinFuture = config.wrap(() -> {
                CompletableFuture<String> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(new Exception("Normal exception"));
                return completableFuture;
            });
            futureResults.add(roundRobinFuture);
        }

        List<Exception> results = new ArrayList<>();
        for(CompletableFuture<String> future : futureResults) {
            try {
                future.get();

            } catch (Exception ex){
                results.add(ex);
            }
        }

        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(0).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(1).getMessage());
        Assert.assertEquals("dk.nversion.LoadBalancerException: All backends suspended", results.get(2).getMessage());
    } */

    // TODO: Implement timeout support - http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html

    // TODO: Implement support for setting a master for retry, fx. in cases where we have a master and several slaves with deplyed replication.

    // TODO: Implement support for caching
    @Test
    public void testLoadBalancerSelfRefreshingCaching() throws Exception {
        String[] urls = new String[] { "url1" };

        LoadBalancer config = LoadBalancer.builder()
                .setEndpointCount(urls.length)
                .setCache(100, TimeUnit.MILLISECONDS, String.class, (index, cache) -> {
                    CompletableFuture<String> valueFuture = new CompletableFuture<String>();
                    valueFuture.complete("ValueToBeCached");
                    return valueFuture;
                })
                .build();
    }

    @Test
    public void testLoadBalancerCaching() throws Exception {
        String[] urls = new String[] { "url1" };

        CompletableFutureCache<String,String> cache = new CompletableFutureCache<>((key, value, store) -> {
            store.put(key, value, 100, TimeUnit.MILLISECONDS);
        });

        LoadBalancer config = LoadBalancer.builder()
                .setEndpointCount(urls.length)
                .build();

            CompletableFuture<String> cachedFuture = config.wrap((index) -> {
                CompletableFuture<String> result = cache.wrap(urls[index], simpleSuccess(urls[index]));
                return result;
        });

    }

}