package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;


public class PlainPubSubTest {
    static final AtomicInteger COUNTER = new AtomicInteger();

    private static final Decoder<Object> decoder = (buf, state) -> {
        try {
            if (COUNTER.getAndIncrement() == 1) {
                System.out.println("sleep");
                Thread.sleep(4_000);
                System.out.println("go work");
            }
        } catch (InterruptedException e) {
            System.out.println("Opps");
        }

        try(ObjectInput inputStream = new ObjectInputStream(new ByteBufInputStream(buf))) {
            return inputStream.readObject();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    };

    private static final Encoder encoder = in -> {
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream result = new ByteBufOutputStream(out);


        try(ObjectOutput outputStream = new ObjectOutputStream(result)) {
            outputStream.writeObject(in);
            outputStream.flush();
            return result.buffer();
        } catch (IOException e) {
            out.release();
            throw new RuntimeException(e);
        }
    };

    static class SlowCodec extends BaseCodec {

        @Override
        public Decoder<Object> getValueDecoder() {
            return decoder;
        }

        @Override
        public Encoder getValueEncoder() {
            return encoder;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConcurrentSkipListSet<String> sentItems = new ConcurrentSkipListSet<>();
        ConcurrentSkipListSet<String> receivedItems = new ConcurrentSkipListSet<>();

        System.out.println("Staring test");

        RedissonClient redissonClient = redissonClient(configSentinel());

        RTopic eventsTopic = redissonClient.getTopic("eventsTopic", new SlowCodec());
        eventsTopic.addListener(String.class, (CharSequence channel, String msg) -> {
            receivedItems.add(msg);
        });

        System.out.println("Starting sending loop");
        for(int i = 0; i<10; i++){
            final String message = UUID.randomUUID().toString();
            eventsTopic.publish(message);
            sentItems.add(message);
            Thread.sleep(10);
        }

        System.out.println("Sent: " + sentItems.size() + ", got: " + receivedItems.size());
        Thread.sleep(10_000);
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
                .setRetryInterval(750)
                .setPingConnectionInterval(connectionPingInterval)
                .setDnsMonitoringInterval(-1)
                .setClientName("test-name")
                .setConnectionMinimumIdleSize(4)
                .setConnectionPoolSize(16);

        return config;
    }

}
