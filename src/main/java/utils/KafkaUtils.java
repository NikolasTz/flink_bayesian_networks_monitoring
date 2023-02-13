package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.DefaultParameters;
import datatypes.input.Input;
import datatypes.message.Message;
import datatypes.message.MessageFGM;
import datatypes.message.OptMessage;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaUtils {

    // *************************************************************************
    //                              KAFKA UTILS
    // *************************************************************************

    // Replace FlinkKafkaConsumer010 with FlinkKafkaConsumer
    //  public static FlinkKafkaConsumer010<T> simpleConsumer(ParameterTool)
    //  public static FlinkKafkaProducer010<T> simpleProducer(ParameterTool)
    //  KafkaDeserializationSchema and KafkaSerializationSchema


    /**
     * Create a simple string kafka consumer
     * which consume records from topic
     * and return a string for each record
     *
     * @param topic input topic
     * @param server boostrap server
     * @param group_id consumer group id
     * @return Return a simple kafka string source consumer
     */
    public static FlinkKafkaConsumer010<String> simpleConsumer(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(),properties);

    }

    /**
     * Create a simple string kafka consumer
     * which consume records from topic
     * and return a string for each record
     *
     * @param params => parameters for setup
     * @return Return a simple kafka string source consumer
     */
    public static FlinkKafkaConsumer010<String> simpleConsumer(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("inputGroupId",DefaultParameters.inputGroupId));

        // Get the topic
        String topic = params.get("inputTopic",DefaultParameters.inputTopic);

        return new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(),properties);

    }

    /**
     * Create a simple string kafka producer for given topic and server
     *
     * @param topic input topic
     * @param server boostrap server
     * @return Return a simple kafka producer
     */
    public static FlinkKafkaProducer010<String> simpleProducer(String topic, String server,String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic, new SimpleStringSchema(),properties);
        // fault tolerance via FlinkKafkaProducer010.Semantic

    }


    public static FlinkKafkaConsumer010<Tuple2<Integer,String>> consumerWithDefault(String topic, String server, String group_id, ExecutionConfig config){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic, new TypeInformationKeyValueSerializationSchema<>(TypeInformation.of(Integer.class),TypeInformation.of(String.class),config),properties);

    }

    public static FlinkKafkaProducer010<Tuple2<Integer,String>> producerWithDefault(String topic, String server, String group_id, ExecutionConfig config){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic, new TypeInformationKeyValueSerializationSchema<>(TypeInformation.of(Integer.class),TypeInformation.of(String.class),config),properties);

    }

    public static FlinkKafkaConsumer010<Tuple2<String,Integer>> consumerWithKafkaDeser(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Tuple2<String,Integer>>(){

                    @Override
                    public TypeInformation<Tuple2<String,Integer>> getProducedType() {
                        return TypeInformation.of(new TypeHint<Tuple2<String,Integer>>(){});
                    }

                    @Override
                    public boolean isEndOfStream(Tuple2<String,Integer> o) { return false; }

                    @Override
                    public Tuple2<String,Integer> deserialize(ConsumerRecord<byte[],byte[]> record) {

                        if(record.key() != null){
                            //ByteBuffer.wrap(bytes).getInt()
                            //Arrays.toString(record.key())
                            return new Tuple2<>(new String(record.value()), Integer.parseInt(new String(record.key())));
                        }

                        return null;
                    }
                },properties);

    }

    public static FlinkKafkaProducer010<Tuple2<String,Integer>> producerWithKafkaSer(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Tuple2<String, Integer>>() {
                    @Override
                    public byte[] serializeKey(Tuple2<String, Integer> record) { return record.f1.toString().getBytes(); }

                    @Override
                    public byte[] serializeValue(Tuple2<String, Integer> record) {
                        return record.f0.getBytes();
                    }

                    @Override
                    public String getTargetTopic(Tuple2<String, Integer> record) { return topic; }
                }
                , properties);

    }

    public static FlinkKafkaProducer010<Tuple3<String,Integer,Long>> producerWithKafkaSerTs(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Tuple3<String,Integer,Long>>() {
                    @Override
                    public byte[] serializeKey(Tuple3<String,Integer,Long> record) { return record.f1.toString().getBytes(); }

                    @Override
                    public byte[] serializeValue(Tuple3<String,Integer,Long> record) {

                        // Initialize byte array
                        byte[] bytes = new byte[0];

                        ObjectMapper mapper = new ObjectMapper();
                        Tuple2<String,Long> value = new Tuple2<>(record.f0,record.f2);

                        try { bytes = mapper.writeValueAsBytes(value); }
                        catch (JsonProcessingException e) { System.out.println(e); }

                        return bytes;
                    }

                    @Override
                    public String getTargetTopic(Tuple3<String,Integer,Long> record) { return topic; }
                }
                , properties);

    }

    public static FlinkKafkaConsumer010<Tuple3<String,Integer,Long>> consumerWithKafkaDeserTs(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Tuple3<String,Integer,Long>>(){

                    @Override
                    public TypeInformation<Tuple3<String,Integer,Long>> getProducedType() {
                        return TypeInformation.of(new TypeHint<Tuple3<String,Integer,Long>>(){});
                    }

                    @Override
                    public boolean isEndOfStream(Tuple3<String,Integer,Long> o) { return false; }

                    @Override
                    public Tuple3<String,Integer,Long> deserialize(ConsumerRecord<byte[],byte[]> record){

                        if(record.key() != null){

                            String tmp = new String(record.value());
                            String value;
                            String ts;

                            if( tmp.contains("COUNTER") ){
                                value = tmp.split(",")[0].split(":")[1];
                                ts = tmp.split(",")[1].split(":")[1];
                            }
                            else{
                                String str = tmp.replaceAll("[{}]","").replaceFirst(",","-");
                                value = str.split(",")[0].split(":")[1].replaceAll("\"","").replaceFirst("-",",");
                                ts = str.split(",")[1].split(":")[1];
                            }

                            return new Tuple3<>(value, Integer.parseInt(new String(record.key())),Long.parseLong(ts));
                        }

                        return null;
                    }
                },properties);

    }


    // INDIVIDUAL counters messages

    public static FlinkKafkaProducer010<Message> producerMessage(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Message>(){

                    @Override
                    public byte[] serializeKey(Message message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(Message message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(Message message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaProducer010<Message> producerMessage(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Message>(){

                    @Override
                    public byte[] serializeKey(Message message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(Message message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(Message message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaConsumer010<Message> consumerMessage(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Message>(){

                    @Override
                    public TypeInformation<Message> getProducedType() {
                        return Types.POJO(Message.class);
                        // or return TypeInformation.of(Message.class);
                    }

                    @Override
                    public boolean isEndOfStream(Message message) { return false; }

                    @Override
                    public Message deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),Message.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }

    public static FlinkKafkaConsumer010<Message> consumerMessage(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Message>(){

                    @Override
                    public TypeInformation<Message> getProducedType() {
                        return Types.POJO(Message.class);
                        // or return TypeInformation.of(Message.class);
                    }

                    @Override
                    public boolean isEndOfStream(Message message) { return false; }

                    @Override
                    public Message deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),Message.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }

    public static FlinkKafkaProducer010<Input> producerInput(String topic, String server, String group_id) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Input>() {

                    @Override
                    public byte[] serializeKey(Input input) { return input.getKey().getBytes(); }

                    @Override
                    public byte[] serializeValue(Input input) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(input); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(Input input) { return topic; }
                }
                , properties);
    }

    public static FlinkKafkaConsumer010<Input> consumerInput(String topic, String server, String group_id) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Input>(){

                    @Override
                    public TypeInformation<Input> getProducedType() {
                        return Types.POJO(Input.class);
                        // or return TypeInformation.of(Input.class);
                    }

                    @Override
                    public boolean isEndOfStream(Input input) { return false; }

                    @Override
                    public Input deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.value() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),Input.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }

    public static FlinkKafkaConsumer010<Input> consumerInput(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("inputGroupId",DefaultParameters.inputGroupId));

        // Get the topic
        String topic = params.get("inputTopic",DefaultParameters.inputTopic);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Input>(){

                    @Override
                    public TypeInformation<Input> getProducedType() {
                        return Types.POJO(Input.class);
                        // or return TypeInformation.of(Message.class); ???
                    }

                    @Override
                    public boolean isEndOfStream(Input input) { return false; }

                    @Override
                    public Input deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),Input.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }


    // OptMessage(IND counters)

    // Producers
    public static FlinkKafkaProducer010<OptMessage> producerOptMessage(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<OptMessage>(){

                    @Override
                    public byte[] serializeKey(OptMessage message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(OptMessage message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(OptMessage message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaProducer010<OptMessage> producerOptMessage(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<OptMessage>(){

                    @Override
                    public byte[] serializeKey(OptMessage message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(OptMessage message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(OptMessage message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaProducer010<OptMessage> producerOptMessageCustomPartitioner(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<OptMessage>() {

                    @Override
                    public byte[] serializeKey(OptMessage message) {
                        return message.getWorker().getBytes();
                    }

                    @Override
                    public byte[] serializeValue(OptMessage message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try {
                            value = mapper.writeValueAsBytes(message);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(OptMessage message) {
                        return topic;
                    }
                }
                , properties
                , new FlinkKafkaPartitioner<OptMessage>() {

                    @Override
                    public int partition(OptMessage optMessage, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                        // return Integer.parseInt(optMessage.getWorker());
                        return new Random().nextInt(partitions.length);
                    }
                });

    }

    // Consumers
    public static FlinkKafkaConsumer010<OptMessage> consumerOptMessage(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<OptMessage>(){

                    @Override
                    public TypeInformation<OptMessage> getProducedType() {
                        return Types.POJO(OptMessage.class);
                        // or return TypeInformation.of(OptMessage.class);
                    }

                    @Override
                    public boolean isEndOfStream(OptMessage message) { return false; }

                    @Override
                    public OptMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),OptMessage.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }

    public static FlinkKafkaConsumer010<OptMessage> consumerOptMessage(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<OptMessage>(){

                    @Override
                    public TypeInformation<OptMessage> getProducedType() {
                        return Types.POJO(OptMessage.class);
                        // or return TypeInformation.of(Message.class);
                    }

                    @Override
                    public boolean isEndOfStream(OptMessage message) { return false; }

                    @Override
                    public OptMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),OptMessage.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }


    // FGM messages

    // Producers
    public static FlinkKafkaProducer010<MessageFGM> producerMessageFGM(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<MessageFGM>(){

                    @Override
                    public byte[] serializeKey(MessageFGM message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(MessageFGM message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(MessageFGM message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaProducer010<MessageFGM> producerMessageFGM(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<MessageFGM>(){

                    @Override
                    public byte[] serializeKey(MessageFGM message) { return message.getWorker().getBytes(); }

                    @Override
                    public byte[] serializeValue(MessageFGM message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(message); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(MessageFGM message) { return topic; }
                }
                ,properties);

    }

    public static FlinkKafkaProducer010<MessageFGM> producerMessageFGMCustomPartitioner(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<MessageFGM>() {

                    @Override
                    public byte[] serializeKey(MessageFGM message) {
                        return message.getWorker().getBytes();
                    }

                    @Override
                    public byte[] serializeValue(MessageFGM message) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try {
                            value = mapper.writeValueAsBytes(message);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(MessageFGM message) {
                        return topic;
                    }
                }
                , properties
                , new FlinkKafkaPartitioner<MessageFGM>() {
                    @Override
                    public int partition(MessageFGM messageFGM, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                        // return Integer.parseInt(messageFGM.getWorker());
                        return new Random().nextInt(partitions.length);
                    }
                });

    }

    // Consumers
    public static FlinkKafkaConsumer010<MessageFGM> consumerMessageFGM(String topic, String server, String group_id){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", group_id);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<MessageFGM>(){

                    @Override
                    public TypeInformation<MessageFGM> getProducedType() {
                        return Types.POJO(MessageFGM.class);
                        // or return TypeInformation.of(Message.class);
                    }

                    @Override
                    public boolean isEndOfStream(MessageFGM message) { return false; }

                    @Override
                    public MessageFGM deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),MessageFGM.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }

    public static FlinkKafkaConsumer010<MessageFGM> consumerMessageFGM(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("feedbackGroupId", DefaultParameters.feedbackGroupId));

        // Get the name of topic
        String topic = params.get("feedbackTopic",DefaultParameters.feedbackTopic);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<MessageFGM>(){

                    @Override
                    public TypeInformation<MessageFGM> getProducedType() {
                        return Types.POJO(MessageFGM.class);
                        // or return TypeInformation.of(Message.class); ???
                    }

                    @Override
                    public boolean isEndOfStream(MessageFGM message) { return false; }

                    @Override
                    public MessageFGM deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),MessageFGM.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }


    // Queries

    // Producer
    public static FlinkKafkaProducer010<Input> producerQuery(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("queryGroupId",DefaultParameters.queryGroupId));

        // Get the name of topic
        String topic = params.get("queryTopic",DefaultParameters.queryTopic);

        return new FlinkKafkaProducer010<>(topic,
                new KeyedSerializationSchema<Input>(){

                    @Override
                    public byte[] serializeKey(Input input) { return input.getKey().getBytes(); }

                    @Override
                    public byte[] serializeValue(Input input) {

                        ObjectMapper mapper = new ObjectMapper();
                        byte[] value = null;

                        try { value = mapper.writeValueAsBytes(input); }
                        catch (JsonProcessingException e) { e.printStackTrace(); }

                        return value;
                    }

                    @Override
                    public String getTargetTopic(Input message) { return topic; }
                }
                ,properties);

    }

    // Consumer
    public static FlinkKafkaConsumer010<Input> consumerQuery(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("queryGroupId",DefaultParameters.queryGroupId));

        // Get the topic
        String topic = params.get("queryTopic",DefaultParameters.queryTopic);

        return new FlinkKafkaConsumer010<>(topic,
                new KafkaDeserializationSchema<Input>(){

                    @Override
                    public TypeInformation<Input> getProducedType() {
                        return Types.POJO(Input.class);
                    }

                    @Override
                    public boolean isEndOfStream(Input input) { return false; }

                    @Override
                    public Input deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                        if(record.key() != null){
                            ObjectMapper mapper = new ObjectMapper();
                            return mapper.readValue(record.value(),Input.class);
                        }

                        return null;
                    }
                }
                ,properties);
    }


    // Coordinator and Worker statistics

    public static FlinkKafkaProducer010<String> workerStatsProducer(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("workerStatsGroupId",DefaultParameters.workerStatsGroupId));

        // Get the topic
        String topic = params.get("workerStatsTopic",DefaultParameters.workerStatsTopic);

        return new FlinkKafkaProducer010<>(topic, new SimpleStringSchema(),properties);
    }

    public static FlinkKafkaProducer010<String> coordinatorStatsProducer(ParameterTool params){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.get("server", DefaultParameters.server));
        properties.setProperty("group.id", params.get("coordinatorStatsGroupId",DefaultParameters.coordinatorStatsGroupId));

        // Get the topic
        String topic = params.get("coordinatorStatsTopic",DefaultParameters.coordinatorStatsTopic);

        return new FlinkKafkaProducer010<>(topic, new SimpleStringSchema(),properties);
    }

}
