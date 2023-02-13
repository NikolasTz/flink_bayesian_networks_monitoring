package kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.message.OptMessage;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OptMessageSerializer implements Serializer<OptMessage> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public byte[] serialize(String topic, OptMessage message) {
        if( message == null ) return null;
        try { return objectMapper.writeValueAsBytes(message); }
        catch (JsonProcessingException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close() { }
}
