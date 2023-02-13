package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Properties;

public class kafkaConsumerWorkerStats {

    public static void main(String[] args) throws IOException {

        // Assign topicName to string variable
        String topicName = "workerStatsALARM";

        // Set up the properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "workerStatsALARM1-group");

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
        /* FileWriter fileWriter0 = new FileWriter("worker_stats_0.txt");
        FileWriter fileWriter1 = new FileWriter("worker_stats_1.txt");
        FileWriter fileWriter2 = new FileWriter("worker_stats_2.txt");
        FileWriter fileWriter3 = new FileWriter("worker_stats_3.txt");
        FileWriter fileWriter4 = new FileWriter("worker_stats_4.txt");
        FileWriter fileWriter5 = new FileWriter("worker_stats_5.txt");
        FileWriter fileWriter6 = new FileWriter("worker_stats_6.txt");
        FileWriter fileWriter7 = new FileWriter("worker_stats_7.txt");*/
        // BufferedWriter writer = new BufferedWriter(fileWriter0);

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

                if(record.value().contains("Worker 0")){
                    Files.write(Paths.get("worker_stats_0.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                    // writer.write(record.value());
                }
                else if(record.value().contains("Worker 1")){
                    Files.write(Paths.get("worker_stats_1.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if(record.value().contains("Worker 2")){
                    Files.write(Paths.get("worker_stats_2.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if(record.value().contains("Worker 3")){
                    Files.write(Paths.get("worker_stats_3.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if(record.value().contains("Worker 4")){
                    Files.write(Paths.get("worker_stats_4.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if(record.value().contains("Worker 5")){
                    Files.write(Paths.get("worker_stats_5.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if(record.value().contains("Worker 6")){
                    Files.write(Paths.get("worker_stats_6.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                else if (record.value().contains("Worker 7")){
                    Files.write(Paths.get("worker_stats_7.txt"),(record.value()+"\n").getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                // writer.write(record.value()+"\n");
            }

            // Commits the offset of record to broker from the poll.
            consumer.commitAsync();
        }

        // Close consumer
        consumer.close();


    }
}
