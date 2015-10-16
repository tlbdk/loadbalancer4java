# loadbalancer4java
[![Build Status](https://travis-ci.org/tlbdk/loadbalancer4java.svg?branch=master)](https://travis-ci.org/tlbdk/loadbalancer4java)

Software load balancer for CompletableFutures in Java, that is meant as a simple replacement for Hystrix and Ribbon when using Java 8.

It provides the following features:

* Load balancing
* Fault tolerance
* Protocol independent

Still needs to be pushed to Nexus:
```xml
<dependency>
    <groupId>dk.nversion</groupId>
    <artifactId>loadbalancer4java</artifactId>
    <version>0.1/version>
</dependency>
```
## How to Use It

Create a List or Array with your endpoints, then create a new LoadBalancer with the preferred configuration:
```java
String[] urls = new String[] { "url1", "url2", "url3" };

LoadBalancer loadBalancer = LoadBalancer.builder()
    // Return indexes for 3 endpoints and do it in a round robin fashion
    .setEndpointCount(urls.length)
    .setPolicy(LoadBalancerPolicy.ROUND_ROBIN)
    // 10 failures in 30 seconds will take the endpoint out of rotation for 60 seconds
    .setMaxFailureRate(10, 30, TimeUnit.SECONDS, 60, TimeUnit.SECONDS)
    // Try to call the endpoints 2 times more if the first call fails
    .setRetryCount(2)
    .build();
```

Wrap the call where you call your backend with the loadBalancer and use the CompletableFuture exactly the same way as before:
```java
// CompletableFuture<String> future = backend.call(urls[0], "request1");
CompletableFuture<String> loadBalancedFuture = loadBalancer.wrap((index, retryCount) -> {
    return backend.call(urls[index], "request1");
});

loadBalancedFuture.whenComplete((response, ex) -> {
    if(ex == null) {
        // This is
    } else {
        // Handle Exception
    }
});
```

## TODO
* Implement caching support like Guava CacheBuilder

