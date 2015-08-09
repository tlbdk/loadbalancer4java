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

    // TODO: Implement monitor
    @Test
    public void testLoadBalancerMonitor() throws Exception {
    /*   String[] urls = new String[] { "url1" };


        LoadBalancer config = LoadBalancer.builder()
                .setEndpointCount(urls.length)
                .setMonitor(30, TimeUnit.SECONDS, 3, 1, 60, TimeUnit.SECONDS, (index) -> {
                    CompletableFuture<Boolean> monitorCheck = new CompletableFuture<Boolean>();
                    // Check urls[index] is up
                    monitorCheck.complete(false);
                    return monitorCheck;
                })
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
        */
    }

}