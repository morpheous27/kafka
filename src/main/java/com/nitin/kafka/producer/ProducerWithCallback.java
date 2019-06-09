package com.nitin.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        // config
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        for(int i=0; i<10; i++){

            record = new ProducerRecord<String, String>("first_topic", "hello world_"+i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("callback function.");
                        System.out.println("topic." + metadata.topic());
                        System.out.println("partition." + metadata.partition());
                        System.out.println("offset." + metadata.offset());
                    }
                }
            });
        }

        producer.flush();
        producer.close();

    }
}
