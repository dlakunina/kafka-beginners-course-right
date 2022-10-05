package dl.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String applicationId = "kafka-demo-streams";

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets_2");
        KStream<String, String> filteredStream = inputTopic.filter((k, v) -> (Integer.valueOf(extractNumber(v)) > 7));
        filteredStream.to("filtered_numbers");
        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start streams application
        kafkaStreams.start();
    }


    private static String extractNumber(String numberJson) {
        try { return JsonParser.parseString(numberJson)
                .getAsJsonObject()
                .get("number")
                .getAsString();
        } catch (NullPointerException e) {
            return "0";
        }
    }

}
