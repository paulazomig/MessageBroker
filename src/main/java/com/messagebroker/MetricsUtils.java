package com.messagebroker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MetricsUtils {
    private static final String BASE_CHARACTER = "a";

    public static String createPayload(int sizeMessage) {
        return BASE_CHARACTER.repeat(sizeMessage);
    }

    public static void registerMetrics(int consumedMessages, long startTime, long endTime) {
        double makespan =  (endTime - startTime);  // Makespan in milliseconds
        double throughput = consumedMessages / makespan;  // Throughput in messages per millisecond

        String filePath = "metrics_makespan_throughput.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            writer.write(makespan + "\t" + throughput + "\n");
        } catch (IOException e) {
            System.err.println("Error writing metrics to file: " + e.getMessage());
        }
    }

    public static void registerMetrics(int consumedMessages, long startTime, long endTime, long p3Time, long p2Time) {
        double makespan =  (endTime - startTime);  // Makespan in milliseconds
        double makespanP3 =  (p3Time - startTime);
        double makespanP2 =  (p2Time - p3Time);
        double makespanP1 =  (endTime - p2Time);
        double throughput = consumedMessages / makespan;  // Throughput in messages per millisecond

        String filePath = "metrics_makespan_throughput.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            writer.write(makespan + "\t" + throughput + "\t" + makespanP3 + "\t" + makespanP2 + "\t" + makespanP1 + "\n");
        } catch (IOException e) {
            System.err.println("Error writing metrics to file: " + e.getMessage());
        }
    }


    public static void registerLatency(Map<String, Object> headers, long endTime) {
        int rand = ThreadLocalRandom.current().nextInt(1, 100);

        if(rand <= 4) {
            long startTime = (long) headers.get("timestamp");
            double latency = (endTime - startTime); //latency in milliseconds

            String filePath = "metrics_latency.txt";

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(startTime + "\t" + latency + "\n");
            } catch (IOException e) {
                System.err.println("Error writing metrics to file: " + e.getMessage());
            }
        }
    }

    public static void registerLatency(int priority, Map<String, Object> headers, long endTime) {
        int rand = ThreadLocalRandom.current().nextInt(1, 100);

        if(rand <= 4) {
            long startTime = (long) headers.get("timestamp");
            double latency = (endTime - startTime); //latency in milliseconds

            String filePath = "metrics_latency.txt";

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(priority + "\t" + startTime + "\t" + latency + "\n");
            } catch (IOException e) {
                System.err.println("Error writing metrics to file: " + e.getMessage());
            }
        }
    }
}
