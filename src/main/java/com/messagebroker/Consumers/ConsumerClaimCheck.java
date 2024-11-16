package com.messagebroker.Consumers;

import com.messagebroker.Constants;
import com.messagebroker.MetricsUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class ConsumerClaimCheck {

    private static int consumedMessages = 0;

    public static void main(String[] args) throws Exception {
        Connection connection = createRabbitMQConnection();
        Channel channel = connection.createChannel();
        Jedis jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT);

        setupRabbitMQ(channel);

        long startTime = System.currentTimeMillis();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            consumedMessages++;
            String claimCheckId = new String(delivery.getBody(), "UTF-8");
            String payload = jedis.get(claimCheckId);
            Map<String, Object> headers = delivery.getProperties().getHeaders();
            MetricsUtils.registerLatency(headers, System.currentTimeMillis());

            if(consumedMessages == Integer.parseInt(args[0])) {
                long endTime = System.currentTimeMillis();
                MetricsUtils.registerMetrics(consumedMessages, startTime, endTime);
                endProgram(channel, connection, jedis);
            }
        };

        channel.basicConsume(Constants.QUEUE_CLAIM_CHECK, true, deliverCallback, consumerTag -> {});
    }

    private static Connection createRabbitMQConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.RABBITMQ_HOST);
        return factory.newConnection();
    }

    private static void setupRabbitMQ(Channel channel) throws Exception {
        channel.exchangeDeclare(Constants.EXCHANGE_CLAIM_CHECK, "fanout");
        channel.queueDeclare(Constants.QUEUE_CLAIM_CHECK, true, false, false, null);
        channel.queueBind(Constants.QUEUE_CLAIM_CHECK, Constants.EXCHANGE_CLAIM_CHECK, "");
    }

    private static void endProgram(Channel channel, Connection connection, Jedis jedis) {
        try {
            channel.close();
            connection.close();
            jedis.close();
        } catch (Exception e) {
            System.err.println("Error ending program: " + e.getMessage());
        }
    }
}
