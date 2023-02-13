package kafka;

import datatypes.input.Input;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;

public class KafkaProducer {

    public static void main(String[] args) throws Exception {

        // number partitions of topic > 1 => bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic test , kafka 0.10.2
        // bin\windows\kafka-topics.bat --describe --zookeeper localhost:2181 --topic test
        // bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
        // bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
        // Range partition [0,num_partitions)

        // Assign topicName to string variable
        String topicName = "sourceHEPAR25M";

        // Create instance for properties to access producer configs
        // Else properties with ProducerConfig
        Properties props = new Properties();

        // Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        // Kafka brokers TUC
        // props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");

        // Assign group_id
        props.put("group.id","sourceHEPAR25M-group");

        // Set acknowledgements for producer requests.
        // The acks config controls the criteria under which requests are considered complete.
        // The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
        props.put("acks", "all");

        // If the request fails, the producer can automatically retry
        props.put("retries", 0);

        // Kafka uses an asynchronous publish/subscribe model.
        // The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server.Turning these records into requests and transmitting them to the cluster.
        // The send() method is asynchronous.
        // When called it adds the record to a buffer of pending record sends and immediately returns.
        // This allows the producer to batch together individual records for efficiency.

        // Specify buffer size in config
        // The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by the batch.size config
        // Controls how many bytes of data to collect before sending messages to the Kafka broker.
        // Set this as high as possible, without exceeding available memory.
        // The default value is 16384.
        props.put("batch.size", 16384);

        // Reduce the number of requests less than 0
        // linger.ms sets the maximum time to buffer data in asynchronous mode
        // By default, the producer does not wait. It sends the buffer any time data is available.
        // E.g: Instead of sending immediately, you can set linger.ms to 5 and send more messages in one batch.
        // This would reduce the number of requests sent, but would add up to 5 milliseconds of latency to records sent, even if the load on the system does not warrant the delay.
        props.put("linger.ms", 0);

        // The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        // key-serializer -> integer
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        // value-serializer -> string
        // props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "kafka.serde.InputSerializer");

        // Producer
        // Producer<Integer, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        Producer<Integer, Input> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // Read the file line by line and send to topic topicName
        BufferedReader br = new BufferedReader(new FileReader("datasets\\data_hepar20_5000000")); // input file as args[0]
        String line = br.readLine();

        // int tmp_counter = 0; // temporary counter
        // int num_partition = 0; // number of partition
        int total_count=0;

        while (line != null && !line.equals("EOF")) {

            // Skip the schema of dataset
            if( total_count == 0 ){
                total_count++;
                line = br.readLine();
                continue;
            }

            // Strips off all non-ASCII characters
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            // Erases all the ASCII control characters
            line = line.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
            // Skip empty lines
            if(line.equals("")) {
                line = br.readLine();
                continue;
            }
            // Skip the schema of dataset
            if(line.equals(new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_hepar2")).readLine())){
                line = br.readLine();
                continue;
            }

            // Print the line
            System.out.println(line);

            // ProducerRecord (string topic, k key, v value)
            // Topic => a topic to assign record.
            // Key => key for the record(included on topic).
            // Value => record contents
            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, new Random().nextInt(4),line);

            // ProducerRecord (string topic, v value)
            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,line);

            int key = new Random().nextInt(8);
            Input input = new Input(line,String.valueOf(key),System.currentTimeMillis());
            ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,key,key,input);
            // ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,new Random().nextInt(16),key,input);
            // ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,input);

            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,line); // Write all tuples to one partition

            // int num_partitions = new Random().nextInt(4);
            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,num_partitions,num_partitions,line);

            // Write records to partition sorted by timestamp
            /* Update counter
            tmp_counter++;

            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,num_partition, line);
            if( tmp_counter == 38){
                // Reset counter
                tmp_counter = 0;
                // Update the number of partitions
                num_partition++;
            }*/

            // int key = new Random().nextInt(12);
            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,key,key,line);

            // ProducerRecord(String topic, Integer partition, K key, V value)
            // ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,new Random().nextInt(4), new Random().nextInt(4)+1,line);

            // Send the record and get the metadata
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());

            // Read the next line
            line = br.readLine();

            total_count++;
        }

        // Write EOF to all partitions
        for(int i=0;i<8;i++){

            // ProducerRecord(String topic, Integer partition, K key, V value)
            Input input = new Input("EOF",String.valueOf(i),System.currentTimeMillis());
            ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,i,i,input);
            // ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,new Random().nextInt(16),i,input);
            // ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,input);

            // Send the record and get the metadata
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());
        }

        // String as input
        /*for(int i=0;i<12;i++){

            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,i,i,"EOF");

            // Send the record and get the metadata
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());
        }*/

        // Write EOF to all partitions for all workers
        /*for(int i=0;i<16;i++){

            for(int j=0;j<8;j++){

                // ProducerRecord(String topic, Integer partition, K key, V value)
                Input input = new Input("EOF",String.valueOf(j),System.currentTimeMillis());
                ProducerRecord<Integer, Input> record = new ProducerRecord<>(topicName,i,j,input);

                // Send the record and get the metadata
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());
            }
        }*/

        System.out.println("Total count : "+total_count);

        // Close BufferedReader and Producer
        br.close();
        producer.close();
    }
}
