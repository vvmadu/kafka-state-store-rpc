package com.hcl.nervIO.api.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static java.text.MessageFormat.format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class APIService {

    public static final Logger logger = LoggerFactory.getLogger(APIService.class);

    @Value("${store.name}") String storeName;
    @Value("${application.context}") String applicationContext;

    @Autowired Serde keySarde;
    @Autowired HostInfo hostInfo;
    @Autowired KafkaStreams kafkaStreams;
    @Autowired RestTemplate restTemplate;

    public Object getById(String id) {
        logger.debug(format("Querying local state store by key as {0}",id));
        ReadOnlyKeyValueStore<String, Object> valueInStore = kafkaStreams.store(storeName,
                QueryableStoreTypes.keyValueStore());

        StreamsMetadata streamsMetadata = kafkaStreams.metadataForKey(storeName, id, keySarde.serializer());
        //make local call first
        if (null !=  streamsMetadata) {
            if (isSameHost(streamsMetadata.hostInfo())) {
                Object valueObj = valueInStore.get(id);
                if (null != valueObj) {
                    if("org.apache.avro.generic.GenericData$Record".equals(valueObj.getClass().getName())) {
                        return ((GenericRecord) valueObj).toString();
                    }
                } else
                    return null;
            } else {
                HostInfo hostInfo = streamsMetadata.hostInfo();
                logger.debug(format("Diverting the request to host: {0} and port: {1}", hostInfo.host(),
                        hostInfo.port()));
                Map<String, Object> uriVariable = new HashMap<>();
                uriVariable.put("productId", id);
                return restTemplate.getForObject(urlPreparation(hostInfo, applicationContext+
                                "/{productId}"), Object.class, uriVariable);
            }
        }
        return null;
    }

    public List getAll() {
        logger.debug(format("Querying all records from it's local store: {0}", storeName));
        List<Object> retList = new ArrayList<>();
        ReadOnlyKeyValueStore<String, Object> valueInStore = kafkaStreams.store(storeName,
                QueryableStoreTypes.keyValueStore());

        KeyValueIterator<String, Object> kvIterator = valueInStore.all();

        while (kvIterator.hasNext()) {
            KeyValue<String, Object> keyVal = kvIterator.next();
            retList.add(((GenericRecord) keyVal.value).toString());
        }
        return retList;
    }

    private String urlPreparation(HostInfo hostInfo, String path) {
        return String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path);
    }

    private boolean isSameHost(HostInfo calledHostInfo) {
        return this.hostInfo.equals(calledHostInfo);
    }
}



// search all node and merge the results
//  new API later
//Gatekeeper for data generation


//https://stackoverflow.com/questions/44810221/setting-partition-strategy-in-a-kafka-connector
//https://github.com/ivangfr/springboot-kafka-connect-streams
//postman for testing

//-------------------------------------------------------------
//no of hits in

//Scope:
//1.

//-------------------------------

// search by sku