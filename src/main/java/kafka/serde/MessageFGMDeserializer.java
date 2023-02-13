package kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.message.MessageFGM;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class MessageFGMDeserializer implements Deserializer<MessageFGM> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public MessageFGM deserialize(String s, byte[] bytes) {

        if( bytes == null ) return null;
        try { return objectMapper.readValue(bytes, MessageFGM.class); }
        catch (IOException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close() {}
}
