package com.messagebroker.Producers;

import com.messagebroker.Constants;
import com.messagebroker.MetricsUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.UUID;


public class ProducerClaimCheck {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.RABBITMQ_HOST);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel(); Jedis jedis = new Jedis(Constants.REDIS_HOST, Constants.REDIS_PORT)) {
            channel.exchangeDeclare(Constants.EXCHANGE_CLAIM_CHECK, "fanout");
            String payload = MetricsUtils.createPayload(Integer.parseInt(args[0]));

            for (int i = 0; i < Integer.parseInt(args[1]); i++) {
                String claimCheckId = storePayload(jedis, payload);
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of("timestamp", System.currentTimeMillis()))
                        .build();

                channel.basicPublish(Constants.EXCHANGE_CLAIM_CHECK, "", props, claimCheckId.getBytes());
            }
        }
    }

    private static String storePayload(Jedis jedis, String payload) {
        String claimCheckId = UUID.randomUUID().toString();
        jedis.set(claimCheckId, payload);

        return claimCheckId;
    }
}