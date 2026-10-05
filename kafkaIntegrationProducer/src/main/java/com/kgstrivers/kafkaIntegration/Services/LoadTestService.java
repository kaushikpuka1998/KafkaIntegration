package com.kgstrivers.kafkaIntegration.Services;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class LoadTestService {

    private static final int TOTAL_REQUESTS = 1_000_000;
    private static final int CONCURRENT_THREADS = 100;

    private final HttpClient httpClient =
            HttpClient.newHttpClient();

    public void startLoadTest() {

        ExecutorService executor =
                Executors.newFixedThreadPool(CONCURRENT_THREADS);

        CountDownLatch doneLatch =
                new CountDownLatch(TOTAL_REQUESTS);

        AtomicInteger success =
                new AtomicInteger();

        AtomicInteger failure =
                new AtomicInteger();

        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_REQUESTS; i++) {

            executor.submit(() -> {

                try {

                    String body = """
                            {
                                "product": "MacBook",
                                "amount": 190000
                            }
                            """;

                    HttpRequest request =
                            HttpRequest.newBuilder()
                                    .uri(URI.create(
                                            "http://localhost:8081/orders"
                                    ))
                                    .header(
                                            "Content-Type",
                                            "application/json"
                                    )
                                    .POST(
                                            HttpRequest.BodyPublishers
                                                    .ofString(body)
                                    )
                                    .build();

                    HttpResponse<String> response =
                            httpClient.send(
                                    request,
                                    HttpResponse.BodyHandlers.ofString()
                            );

                    if (response.statusCode() >= 200
                            && response.statusCode() < 300) {

                        success.incrementAndGet();

                    } else {

                        failure.incrementAndGet();
                    }

                } catch (Exception e) {

                    failure.incrementAndGet();

                } finally {

                    doneLatch.countDown();
                }
            });
        }

        try {

            doneLatch.await();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

        } finally {

            executor.shutdown();
        }

        long end = System.currentTimeMillis();

        double seconds =
                (end - start) / 1000.0;

        System.out.println(
                "========== LOAD TEST =========="
        );

        System.out.println(
                "Total requests : " + TOTAL_REQUESTS
        );

        System.out.println(
                "Successful     : " + success.get()
        );

        System.out.println(
                "Failed         : " + failure.get()
        );

        System.out.println(
                "Time           : " + seconds + " seconds"
        );

        System.out.println(
                "Requests/sec   : " +
                        TOTAL_REQUESTS / seconds
        );
    }
}
