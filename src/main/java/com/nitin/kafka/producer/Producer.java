package com.nitin.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        // config
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        /*This is asynchronous.
        This statement executes and the main program does not wait for this to produce the data.
        Hence we need to flush and close the producer.*/
        producer.send(record);

        producer.flush();
        producer.close();

    }
}
