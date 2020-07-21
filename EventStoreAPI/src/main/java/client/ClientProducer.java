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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class ClientProducer {

    public static void main(String... args) throws Exception {
        String schemaStr = "{\"namespace\":\"com.hcl.product.model\", \"type\":\"record\", \"name\":\"ProductData\",\"fields\":[{\"name\":\"productId\",\"type\":\"string\"},{\"name\":\"productName\",\"type\":\"string\"},{\"name\":\"productValue\",\"type\":\"double\"},{\"name\":\"discount\",\"type\":[\"double\",\"null\"]}, {\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"location\",\"type\":\"string\"}]}";
        Producer producer = null;

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        GenericRecord avroRecord = new GenericData.Record(schema);
        producer = createProducer();
        long num = 0l;

        while (true) {
            avroRecord.put("productId", "prod"+num);
            avroRecord.put("productName", "prodname"+num);
            avroRecord.put("productValue", Double.parseDouble("100"));
            avroRecord.put("discount", Double.parseDouble("10"));
            avroRecord.put("sku", "sku");
            avroRecord.put("location", "bengaluru");

            produceRecord(avroRecord, "prod"+num, producer);
            num++;
        }
    }

    static Producer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    static void produceRecord(GenericRecord customerModel, String key, Producer<String, GenericRecord> producer) {
        producer.send(new ProducerRecord("INPUT", key, customerModel));
        producer.flush();
    }
}
