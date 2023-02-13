package kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.message.Message;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public Message deserialize(String topic, byte[] bytes) {

        if( bytes == null ) return null;
        try { return objectMapper.readValue(bytes, Message.class); }
        catch (IOException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close() { }
}
