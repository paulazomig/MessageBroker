package com.messagebroker.Producers;

import com.messagebroker.Constants;
import com.messagebroker.MetricsUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Map;


public class ProducerPubSub {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.RABBITMQ_HOST);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(Constants.EXCHANGE_PUBSUB, "fanout");
            String payload = MetricsUtils.createPayload(Integer.parseInt(args[0]));

            for(int i=0; i < Integer.parseInt(args[1]); i++) {
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of("timestamp", System.currentTimeMillis()))
                        .build();

                channel.basicPublish(Constants.EXCHANGE_PUBSUB, "", props, payload.getBytes());
            }
        }
    }
}
