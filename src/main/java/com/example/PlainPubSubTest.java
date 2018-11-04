package com.example;

import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;

import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

public class PlainPubSubTest {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentSkipListSet<String> sentItems = new ConcurrentSkipListSet<>();
        ConcurrentSkipListSet<String> receivedItems = new ConcurrentSkipListSet<>();

        System.out.println("Staring test");

        RedissonClient redissonClient = redissonClient(configSentinel());

        RTopic eventsTopic = redissonClient.getTopic("eventsTopic");
        eventsTopic.addListener(String.class, (CharSequence channel, String msg) -> {
            receivedItems.add(msg);
        });

        System.out.println("Starting sending loop");
        for(int i = 0; i<1000; i++){
            final String message = UUID.randomUUID().toString();
            eventsTopic.publish(message);
            sentItems.add(message);
            Thread.sleep(10);
        }

        System.out.println("Sent: " + sentItems.size() + ", got: " + receivedItems.size());
        Thread.sleep(1000);
        System.out.println("Sent: " + sentItems.size() + ", got: " + receivedItems.size());

        redissonClient.shutdown();
    }

    private static RedissonClient redissonClient(Config config){
        return Redisson.create(config);
    }

    private static Config configSentinel(){
        Config config = new Config();
        config.setCodec(new SerializationCodec());
        int connectionPingInterval = 50; //Changing to 0 eliminates issue

        config.useSingleServer()
                .setAddress("redis://localhost:6379")
                .setConnectTimeout(20_000)
                .setTimeout(25_000)
                .setRetryInterval(750)
                .setPingConnectionInterval(connectionPingInterval)
                .setDnsMonitoringInterval(-1)
                .setClientName("test-name")
                .setConnectionMinimumIdleSize(4)
                .setConnectionPoolSize(16);

        return config;
    }
}