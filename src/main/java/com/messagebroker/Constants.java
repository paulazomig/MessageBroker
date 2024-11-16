package com.messagebroker;

public final class Constants {

    private Constants() {}

    public static final String RABBITMQ_HOST = "localhost";
    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;

    public static final String EXCHANGE_PUBSUB = "exchange-pubsub";

    public static final String EXCHANGE_CC = "exchange-cc";
    public final static String QUEUE_CC = "queue-cc";
    public static final String ROUTING_KEY_CC = "key-cc";

    public static final String EXCHANGE_PRIORITY = "exchange-pq";
    public static final String QUEUE_PRIORITY = "queue-pq";

    public static final String EXCHANGE_CLAIM_CHECK = "exchange-claim-check";
    public static final String QUEUE_CLAIM_CHECK = "queue-claim-check";
}