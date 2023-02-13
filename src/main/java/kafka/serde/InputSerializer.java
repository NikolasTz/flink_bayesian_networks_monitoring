package kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.input.Input;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class InputSerializer implements Serializer<Input>{

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b){}

    @Override
    public byte[] serialize(String topic, Input input) {

        if( input == null ) return null;
        try { return objectMapper.writeValueAsBytes(input); }
        catch (JsonProcessingException e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public void close(){}
}
