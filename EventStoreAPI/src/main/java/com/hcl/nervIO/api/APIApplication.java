package com.hcl.nervIO.api;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.RestTemplate;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static java.text.MessageFormat.format;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@EnableSwagger2
@ComponentScan(value = "com.hcl.nervIO.api.*")
@SpringBootApplication
public class APIApplication implements CommandLineRunner {

    public static final Logger logger = LoggerFactory.getLogger(APIApplication.class);

    @Value("${schema}")
    String schemaStr;

    @Value("${application.name}")
    String applicationName;

    @Value("${kafka.bootstrap.server}")
    String bootStrapUrl;

    @Value("${topic.name}")
    String topicName;

    @Value("${store.name}")
    String storeName;

    @Value("${schema.registry.url}")
    String schemaRegistryUrl;

    @Value("${client.id}")
    String clientId;

    @Value("${group.id}")
    String groupId;
	
	@Value("${restAPI.host.port}")
    String restAPIHostPort;

    public static void main(String[] args) {
        SpringApplication.run(APIApplication.class, args);
    }

    @Bean
    public Object genericSchema() {
        Schema.Parser parser = new org.apache.avro.Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        return schema;
    }

    @Bean
    public Object genericRecord() {
        GenericData.Record genericRecord = new GenericData.Record((Schema) genericSchema());
        return genericRecord;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public Serde keySarde() {
        return new org.apache.kafka.common.serialization.Serdes.StringSerde();
    }
    @Bean
    public Serde valueSarde() {
        return new io.confluent.kafka.streams.serdes.avro.GenericAvroSerde();
    }

    @Bean
    public HostInfo hostInfo() {
        return new HostInfo(restAPIHostPort.substring(0, restAPIHostPort.indexOf(":")),
                Integer.parseInt(restAPIHostPort.substring(restAPIHostPort.indexOf(":")+1)));
    }

    @Bean
    Properties streamsConfig() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put("application.id", applicationName);
        streamsConfiguration.put("bootstrap.servers", bootStrapUrl);
        streamsConfiguration.put("application.server", restAPIHostPort);
        streamsConfiguration.put("commit.interval.ms",500);
        streamsConfiguration.put("metadata.max.age.ms", 100);

        streamsConfiguration.put("default.key.serde", keySarde().getClass().getName());
        streamsConfiguration.put("default.value.serde", valueSarde().getClass().getName());
        streamsConfiguration.put("client.id", clientId);
        streamsConfiguration.put("group.id", groupId);
        streamsConfiguration.put("schema.registry.url", schemaRegistryUrl);

        return streamsConfiguration;
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public Topology topology() {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);
        GenericAvroSerde genericRecordSerde = new GenericAvroSerde();
        genericRecordSerde.configure(serdeConfig, false);

        StreamsBuilder streamsBuilder = streamsBuilder();
        KTable<Object, Object> kTable = streamsBuilder.table(topicName, Consumed.with(keySarde(), genericRecordSerde));

        kTable.mapValues(valueMapper(),
                Materialized.<Object, Object, KeyValueStore<Bytes, byte[]>>as(storeName).withKeySerde(keySarde()).withValueSerde(genericRecordSerde));

        Topology topology = streamsBuilder.build();
        return topology;
    }

    @Bean
    public ValueMapper valueMapper() {
        return new ValueMapper<Object,Object>() {
            @Override
            public Object apply(Object value) {
                return (GenericRecord) value;
            }
        };
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        KafkaStreams kafkaStreams = new KafkaStreams( topology(), streamsConfig());
        return kafkaStreams;
    }

    @Override
    public void run(String... arg0) {
        KafkaStreams kafkaStreams = kafkaStreams();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        logger.info("Stream engine initialized and started");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kafkaStreams.close();
            } catch (final Exception e) {
                logger.error(format("Error while shuttingdown the engine, error: {0}", e.toString()));
            }
        }));
    }
}