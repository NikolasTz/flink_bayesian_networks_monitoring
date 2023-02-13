package kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.message.MessageFGM;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MessageFGMSerializer implements Serializer<MessageFGM> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {}


    @Override
    public byte[] serialize(String s, MessageFGM messageFGM) {
        if( messageFGM == null ) return null;
        try { return objectMapper.writeValueAsBytes(messageFGM); }
        catch (JsonProcessingException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close() {}

}
