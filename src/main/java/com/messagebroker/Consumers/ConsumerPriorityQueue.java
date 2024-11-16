package com.messagebroker.Consumers;

import com.messagebroker.Constants;
import com.messagebroker.MetricsUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Map;


public class ConsumerPriorityQueue {

    private static int consumedMessages = 0;
    private static long p3Time;
    private static long p2Time;
    private static int oldPriority = 3;
    private static int newPriority = 3;

    public static void main(String[] args) throws Exception {
        Connection connection = createRabbitMQConnection();
        Channel channel = connection.createChannel();

        setupRabbitMQ(channel);

        long startTime = System.currentTimeMillis();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            consumedMessages++;
            String message = new String(delivery.getBody(), "UTF-8");
            oldPriority = newPriority;
            newPriority = delivery.getProperties().getPriority();

            Map<String, Object> headers = delivery.getProperties().getHeaders();
            MetricsUtils.registerLatency(newPriority, headers, System.currentTimeMillis());

            setPrioritiesTime();

            if(consumedMessages == Integer.parseInt(args[0])) {
                long endTime = System.currentTimeMillis();
                MetricsUtils.registerMetrics(consumedMessages, startTime, endTime, p3Time, p2Time);
                endProgram(channel, connection);
            }
        };

        channel.basicConsume(Constants.QUEUE_PRIORITY, true, deliverCallback, consumerTag -> {
        });
    }

    private static void setPrioritiesTime() {
        if(oldPriority == 3 && newPriority == 2)
            p3Time = System.currentTimeMillis();
        if(oldPriority == 2 && newPriority == 1)
            p2Time = System.currentTimeMillis();
    }

    private static Connection createRabbitMQConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.RABBITMQ_HOST);
        return factory.newConnection();
    }

    private static void setupRabbitMQ(Channel channel) throws Exception {
        channel.exchangeDeclare(Constants.EXCHANGE_PRIORITY, "fanout");
        channel.queueDeclare(Constants.QUEUE_PRIORITY, true, false, false, Map.of("x-max-priority", 3));
        channel.queueBind(Constants.QUEUE_PRIORITY, Constants.EXCHANGE_PRIORITY, "");
    }

    private static void endProgram(Channel channel, Connection connection) {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            System.err.println("Error ending program: " + e.getMessage());
        }
    }
}
