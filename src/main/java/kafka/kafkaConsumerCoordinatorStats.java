package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class kafkaConsumerCoordinatorStats {

    public static void main(String[] args) throws IOException {

        // Assign topicName to string variable
        String topicName = "fd2HEPAR2500K";

        // Set up the properties
        // If you want to consume from beginning then change the group id(equivalent with new consumer)
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        // props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        props.setProperty("group.id", "fd2HEPAR2500KEW-group");

        // Enable auto commit for offsets to false
        props.setProperty("enable.auto.commit", "false");

        // Return how often updated consumed offsets are written to Kafka
        props.setProperty("auto.commit.interval.ms", "1000");

        // Automatically reset the offset when there is no initial offset in Kafka or if the current offset does not exist any more on the server
        // Earliest: automatically reset the offset to the earliest offset
        props.setProperty("auto.offset.reset","earliest");

        // Key,value deserializer
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //  Create a kafka consumer
        Consumer<Integer, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        // Subscribe to the topicName topic
        consumer.subscribe(Collections.singletonList(topicName));

        // Writer
        BufferedWriter writer = new BufferedWriter(new FileWriter("coordinator_stats.txt"));

        int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {

            ConsumerRecords<Integer, String> records = consumer.poll(1000);

            if (records.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp){
                    System.out.println("Timeout.No available data");
                    break;
                }
                else continue;
            }

            for (ConsumerRecord<Integer, String> record : records) {
                // System.out.printf("offset = %d, key = %s, value = %s, partition = %d\n", record.offset(), record.key(), record.value(), record.partition());
                writer.write(record.value()+"\n");
                // writer.write("offset = "+record.offset()+", key = "+record.key()+", value = "+record.value()+" , partition = "+record.partition()+"\n");
            }

            // Commits the offset of record to broker from the poll.
            consumer.commitAsync();
        }

        // Close consumer
        consumer.close();


    }

}
