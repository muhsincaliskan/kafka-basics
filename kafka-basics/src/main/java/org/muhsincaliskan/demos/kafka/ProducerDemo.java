package org.muhsincaliskan.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo
{
    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        System.out.println("hello");


        Properties properties = new Properties();
        // connect to localhost
        //properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //connect to conduktor pg
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"63hiUAoLZfKT9iLvNDA0kz\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2M2hpVUFvTFpmS1Q5aUx2TkRBMGt6Iiwib3JnYW5pemF0aW9uSWQiOjc0NDEzLCJ1c2VySWQiOjg2NTYzLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyOWU2OGJhNS1hNzhiLTQ5NTgtODU5MC1iYjk4MzdlOGRkODUifX0.T83kW8CNqCtgMolR62wB5-7QK3KGsVl9_Cb40I8By-I\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //set producer producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String,String> producerRecord=
                new ProducerRecord<>("demo_kafka","hello world");
        // send data
        producer.send(producerRecord);
        // flush and close producer

        //tell the producer to send all data and until done * synchronized
        producer.flush();

        producer.close();
    }
}
