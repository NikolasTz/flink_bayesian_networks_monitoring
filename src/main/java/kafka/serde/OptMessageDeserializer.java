package kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.message.OptMessage;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class OptMessageDeserializer implements Deserializer<OptMessage> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public OptMessage deserialize(String topic, byte[] bytes) {

        if( bytes == null ) return null;
        try { return objectMapper.readValue(bytes, OptMessage.class); }
        catch (IOException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close() { }
}
