package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import config.CBNConfig;
import config.Config;
import config.FGMConfig;
import datatypes.NodeBN_schema;
import datatypes.input.Input;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static config.InternalConfig.coordinatorKey;
import static datatypes.NodeBN_schema.findIndexNode;
import static operators.Source.QuerySource.calculateProbability;
import static operators.StrToCounter.getQueries;
import static utils.Util.convertToString;

public class KafkaProducerEvents {

    public static void main(String[] args) throws Exception {

        // number partitions of topic > 1 => bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic test , kafka 0.10.2
        // bin\windows\kafka-topics.bat --describe --zookeeper localhost:2181 --topic test
        // bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
        // bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
        // Range partition [0,num_partitions)

        ParameterTool params = ParameterTool.fromArgs(args);
        // CBNConfig config = new CBNConfig(params);
        FGMConfig config = new FGMConfig(params);

        // Initialize producer
        ProducerEvents producerEvents = new ProducerEvents(config);

        // Send events to kafka topic
        // producerEvents.produceEvents("querySource");
        // producerEvents.produceEvents("query4","querySource");

        // producerEvents.produceEventsMod("queryHEPAR24","querySource_hepar2");
        // producerEvents.produceEventsMod("queryALARM1","querySource_alarm");
        producerEvents.produceEventsMod("queryLINK1","querySource_link");

        // producerEvents.produceEventsMod("query1","querySource_hepar2");

    }

    public static class ProducerEvents{

        private final Config config; // Configuration
        private final long size; // The size of events
        private final List<String> datasetSchema; // The schema of dataset
        private Properties props; // Properties of producer

        // Constructors
        public ProducerEvents(CBNConfig config){

            // Initialize the properties from producer
            initializeProducerProperties();

            // Initialize the rest fields
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }
        public ProducerEvents(FGMConfig config){

            // Initialize the properties from producer
            initializeProducerProperties();

            // Initialize the rest fields
            this.config = config;
            size = config.queriesSize();
            datasetSchema = Arrays.asList(config.datasetSchema().split(","));
        }

        // Getters
        public long getSize() { return size; }
        public List<String> getDatasetSchema() { return datasetSchema; }
        public Config getConfig() { return config; }
        public Properties getProps() { return props; }

        // Setters
        public void setProps(Properties props) { this.props = props; }

        // Methods
        public void initializeProducerProperties(){

            // Create instance for properties to access producer configs
            // Else properties with ProducerConfig
            props = new Properties();

            // Assign localhost id
            props.put("bootstrap.servers", "localhost:9092");
            // props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");

            // Assign group_id
            props.put("group.id","source-group");

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

            // value-serializer -> Input class
            props.put("value.serializer", "kafka.serde.InputSerializer");
        }

        // Generate events to querySource topic and file
        public void produceEvents(String querySource) throws IOException, ExecutionException, InterruptedException {

            ObjectMapper objectMapper = new ObjectMapper();

            // Unmarshall JSON object as array of NodeBN_schema
            BufferedWriter writer = new BufferedWriter(new FileWriter("querySource"));
            // NodeBN_schema[] nodes = objectMapper.readValue(new File(config.BNSchema()), NodeBN_schema[].class); // Read value from file or url
            NodeBN_schema[] nodes = objectMapper.readValue(config.BNSchema(), NodeBN_schema[].class); // Read value from string

            // Create the producer
            Producer<Integer,Input> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

            int count = 0;
            double prob;

            while( count < this.getSize() ){

                // Generate event
                ArrayList<String> output;
                output = generateEvent(nodes);

                // Calculate the log probability for output
                prob = calculateProbability(output,datasetSchema,nodes);

                // Check the probability(keep highly likely events)
                // >= log(0.01) or <= 0
                if ( prob <= 0 ) {

                    // Create the output-event(output follow the schema of datasetSchema)
                    Input input = new Input(convertToString(output), coordinatorKey, System.currentTimeMillis());

                    // ProducerRecord (string topic, k key, v value)
                    // Topic => a topic to assign record.
                    // Key => key for the record(included on topic).
                    // Value => record contents
                    ProducerRecord<Integer,Input> record = new ProducerRecord<>(querySource,input);

                    // Send the record and get the metadata
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());

                    // Write the line to file
                    writer.write(input +"\n");

                    // Increment the count
                    count++;
                }
            }

            System.out.println("Total count is : "+count);
            writer.close();

        }

        // Read events from file and write to querySource
        public void produceEvents(String querySource,String file) throws IOException, ParseException, ExecutionException, InterruptedException {

            // Create the producer
            Producer<Integer,Input> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line = br.readLine();
            int count = 0;

            while( line != null ){

                // Process the line

                // Split the lines based on colon
                String[] splitsLine1 = line.replaceAll("\0","").split(":");

                // Get the value
                String value = splitsLine1[1].substring(0,splitsLine1[1].lastIndexOf(",")).trim();

                // Get the timestamp
                String mileSec = splitsLine1[splitsLine1.length-1];
                if(mileSec.split("\\.")[1].length() == 2){ mileSec = mileSec.trim().concat("0"); }
                else if(mileSec.split("\\.")[1].length() == 1){ mileSec = mileSec.trim().concat("0").concat("0"); }

                String timestamp = splitsLine1[splitsLine1.length-3]+":"+splitsLine1[splitsLine1.length-2]+":"+mileSec;
                timestamp = timestamp.trim();

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                Date date = simpleDateFormat.parse(timestamp);
                long ts = date.getTime();

                // Create the output-event(output follow the schema of datasetSchema)
                Input input = new Input(value, coordinatorKey, ts);

                // ProducerRecord (string topic, k key, v value)
                // Topic => a topic to assign record.
                // Key => key for the record(included on topic).
                // Value => record contents
                ProducerRecord<Integer,Input> record = new ProducerRecord<>(querySource,input);

                // Send the record and get the metadata
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());

                // Update the count
                count++;

                // Read the next line
                line = br.readLine();
            }

            System.out.println("Total count : "+ count);

            // Close the buffer
            br.close();

        }

        // Read events from file and write to querySource , first read the queries
        public void produceEventsMod(String querySource,String file) throws IOException, ParseException, ExecutionException, InterruptedException {

            // Create the producer
            Producer<Integer,Input> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

            List<Input> queries = getQueries(file);
            int count = 0;
            for(Input query:queries){

                // ProducerRecord (string topic, k key, v value)
                // Topic => a topic to assign record.
                // Key => key for the record(included on topic).
                // Value => record contents
                query.setValue(query.getValue().replace(",",", "));
                ProducerRecord<Integer,Input> record = new ProducerRecord<>(querySource,0,query);

                // Send the record and get the metadata
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Record(key=%d value=%s) " + "Meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.partition(), metadata.offset());

                // Update the count
                count++;
            }

            System.out.println("Total count : "+ count);

        }

        // Generate query(not require topological order)-Same as method on QuerySource
        public ArrayList<String> generateEvent(NodeBN_schema[] nodes){

            ArrayList<String> output = new ArrayList<>();
            for (String node : datasetSchema) {
                int index = findIndexNode(node, nodes);
                if (index != -1) output.add(generateValue(nodes[index])); // Generate the value
            }

            return output;
        }

        // Generate uniformly at random a value for the node
        public String generateValue(NodeBN_schema node){

            Random rand = new Random();
            int index = rand.nextInt(node.getValues().length);
            return node.getValues()[index];
        }

    }

}
