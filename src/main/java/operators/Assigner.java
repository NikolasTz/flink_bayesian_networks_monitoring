package operators;

import datatypes.input.Input;
import datatypes.message.Message;
import datatypes.message.MessageFGM;
import datatypes.message.OptMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;


public class Assigner {

    // A simple custom ascending assigner for String class
    public static class SimpleAssigner implements AssignerWithPeriodicWatermarks<String> {

        private long currentMaxTimestamp;

        // Constructors
        public SimpleAssigner(){ this.currentMaxTimestamp = 0L; }

        public SimpleAssigner(long startedTimestamp){ this.currentMaxTimestamp = startedTimestamp; }

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(currentMaxTimestamp); }

        @Override
        public long extractTimestamp(String input, long l) {
            currentMaxTimestamp = Math.max(Long.parseLong(input.split(",")[1]), currentMaxTimestamp);
            return currentMaxTimestamp;
        }
    }

    // Assigner with periodic watermarks for String class
    public static class SimpleAssignerInput implements AssignerWithPeriodicWatermarks<String>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MIN_VALUE); }

        @Override
        public long extractTimestamp(String input, long l) { return System.currentTimeMillis(); }
    }

    // Ascending Assigner for String class
    public static class SimpleAscendingAssigner extends AscendingTimestampExtractor<String> {
        @Override
        public long extractAscendingTimestamp(String input) { return Long.parseLong(input.split(",")[1]); }
    }

    // Ascending assigner for Message class
    public static class MessageAssigner implements AssignerWithPeriodicWatermarks<Message>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(Message message, long l) { return message.getTimestamp(); }
    }

    /*// Ascending assigner for Input class
    public static class InputAssigner implements AssignerWithPeriodicWatermarks<Input>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(Input input, long l) { return input.getTimestamp(); }
    }

    // Ascending assigner for MessageFGM class
    public static class MessageFGMAssigner implements AssignerWithPeriodicWatermarks<MessageFGM>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(MessageFGM message, long l) { return message.getTimestamp(); }
    }

    // Ascending assigner for OptMessage class
    public static class OptMessageAssigner implements AssignerWithPeriodicWatermarks<OptMessage>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(OptMessage message, long l) { return message.getTimestamp(); }
    }

    // A simple custom ascending assigner for String class
    public static class StringAssigner implements AssignerWithPeriodicWatermarks<String> {

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(String str, long l) { return System.currentTimeMillis(); }

    }

    // Ascending assigner for Query source(Input class)
    public static class QueryAssigner implements AssignerWithPeriodicWatermarks<Input>{

        @Override
        public Watermark getCurrentWatermark() { return new Watermark(Long.MAX_VALUE); }

        @Override
        public long extractTimestamp(Input input, long l) { return input.getTimestamp(); }
    }*/

    // Ascending assigner for Input class
    public static class InputAssigner extends AscendingTimestampExtractor<Input>{

        @Override
        public long extractAscendingTimestamp(Input input) { return input.getTimestamp(); }

    }

    // Ascending assigner for MessageFGM class
    public static class MessageFGMAssigner extends AscendingTimestampExtractor<MessageFGM>{

        @Override
        public long extractAscendingTimestamp(MessageFGM message) { return message.getTimestamp(); }
    }

    // Ascending assigner for OptMessage class
    public static class OptMessageAssigner extends AscendingTimestampExtractor<OptMessage>{

        @Override
        public long extractAscendingTimestamp(OptMessage message) { return message.getTimestamp(); }
    }

    // A simple custom ascending assigner for String class
    public static class StringAssigner extends AscendingTimestampExtractor<String> {

        @Override
        public long extractAscendingTimestamp(String str) { return System.currentTimeMillis(); }

    }

    // Ascending assigner for Query source(Input class)
    public static class QueryAssigner extends AscendingTimestampExtractor<Input>{

        @Override
        public long extractAscendingTimestamp(Input input) { return input.getTimestamp(); }
    }

    // Ascending assigner for Worker/Coordinator statistics(String class)
    public static class StatsAssigner extends AscendingTimestampExtractor<String>{

        @Override
        public long extractAscendingTimestamp(String string) { return Long.MIN_VALUE; }
    }

}
