package kafka;

import datatypes.message.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) throws IOException {

        // Assign topicName to string variable
        String topicName = "hepar2ERSTUVX";

        // Create instance for properties to access producer configs
        // Else properties with ConsumerConfig
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        // props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        props.setProperty("group.id", "hepar2ERSTUVX-group");

        // Enable auto commit for offsets
        props.setProperty("enable.auto.commit", "false");

        // Return how often updated consumed offsets are written to Kafka
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("auto.offset.reset","earliest");

        // Key,value deserializer
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "kafka.serde.OptMessageDeserializer");

        //  Create a kafka consumer
        Consumer<String, Message> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        // Subscribe to the topicName topic
        consumer.subscribe(Collections.singletonList(topicName));

        // Writer
        BufferedWriter writer = new BufferedWriter(new FileWriter("fd_stats.txt"));

        int giveUp = 100;
        int noRecordsCount = 0;

        while(true) {

            ConsumerRecords<String, Message> records = consumer.poll(100);

            if (records.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord<String, Message> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s, partition = %d\n", record.offset(), record.key(), record.value(), record.partition());
                writer.write("offset = "+record.offset()+", key = "+record.key()+", value = "+record.value()+" , partition = "+record.partition()+"\n");
            }

            consumer.commitAsync();
        }

        // Close consumer
        consumer.close();


    }
}
