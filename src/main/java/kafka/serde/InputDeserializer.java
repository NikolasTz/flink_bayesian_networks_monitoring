package kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.input.Input;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class InputDeserializer implements Deserializer<Input> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public Input deserialize(String topic, byte[] bytes) {

        if( bytes == null ) return null;
        try { return objectMapper.readValue(bytes,Input.class); }
        catch (IOException e) { e.printStackTrace(); }
        return null;

    }

    @Override
    public void close(){}

}
