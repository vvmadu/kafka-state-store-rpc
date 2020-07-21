package client;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HTTPClient {

    public static void main(String... args) throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> uriVariable = new HashMap<>();
        uriVariable.put("productId", args[0]);
        System.out.println(restTemplate.getForObject("http://localhost:8889/products/{productId}", Object.class,
                uriVariable));
    }
}
