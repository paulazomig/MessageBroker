package com.messagebroker.Consumers;

import com.messagebroker.Constants;
import com.messagebroker.MetricsUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Map;


public class ConsumerPubSub {

    private static int consumedMessages = 0;

    public static void main(String[] args) throws Exception {
        Connection connection = createRabbitMQConnection();
        Channel channel = connection.createChannel();

        String tempQueue = channel.queueDeclare().getQueue();
        setupRabbitMQ(tempQueue, channel);

        long startTime = System.currentTimeMillis();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            consumedMessages++;
            String message = new String(delivery.getBody(), "UTF-8");
            Map<String, Object> headers = delivery.getProperties().getHeaders();
            MetricsUtils.registerLatency(headers, System.currentTimeMillis());

            if(consumedMessages == Integer.parseInt(args[0])) {
                long endTime = System.currentTimeMillis();
                MetricsUtils.registerMetrics(consumedMessages, startTime, endTime);
                endProgram(channel, connection);
            }
        };

        channel.basicConsume(tempQueue, true, deliverCallback, consumerTag -> {});
    }

    private static Connection createRabbitMQConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.RABBITMQ_HOST);
        return factory.newConnection();
    }

    private static void setupRabbitMQ(String tempQueue, Channel channel) throws Exception {
        channel.exchangeDeclare(Constants.EXCHANGE_PUBSUB, "fanout");
        channel.queueBind(tempQueue, Constants.EXCHANGE_PUBSUB, "");
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
