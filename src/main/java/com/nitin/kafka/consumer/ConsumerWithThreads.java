package com.nitin.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerWithThreads {

    public static void main(String[] args) {

        // create consumer configs
        Properties properties = getProperties();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new ConsumerThread(countDownLatch, properties));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown call received");
            executorService.shutdownNow();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    private static Properties getProperties() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}


class ConsumerThread implements Runnable {

    CountDownLatch countDownLatch;
    KafkaConsumer<String, String> consumer;

    ConsumerThread(CountDownLatch countDownLatch, Properties properties) {
        this.countDownLatch = countDownLatch;
        consumer = new KafkaConsumer<String, String>(properties);
    }


    @Override
    public void run() {

        String topic = "first_topic";
        try {
            //consumer
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("key " + record.key());
                    System.out.println("value " + record.value());
                    System.out.println("partition " + record.partition());

                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred");
            System.out.println(e);
        }finally {
            countDownLatch.countDown();
        }
    }

    public void shutdown() {
        try {
            consumer.wakeup();
        }catch (WakeupException e)
        {
            System.out.println("Wake up exception occurred");
            System.out.println(e);
        }
        finally {
            countDownLatch.countDown();
        }
    }
}
