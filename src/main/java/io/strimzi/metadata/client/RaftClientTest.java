package io.strimzi.metadata.client;

import kafka.raft.*;
import kafka.server.KafkaConfig;
import kafka.server.MetaProperties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.*;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class RaftClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(RaftClientTest.class);

    public static TopicPartition createMetadataTopicPartition(){
        String metadataTopicName = "__cluster_metadata";
        return new TopicPartition(metadataTopicName, 0);
    }

    public static KafkaConfig loadConfig(String serverPropsFilePath) throws IOException {
        LOG.info("Reading config from: " + serverPropsFilePath);
        Properties props = Utils.loadProps(serverPropsFilePath);
        return KafkaConfig.fromProps(props,true);
    }

    public static void runKRaftManager(String serverPropsFilePath, String clusterId) throws IOException, InterruptedException {

        MetaProperties metaProps = new MetaProperties(clusterId, -1);

        KafkaConfig config = loadConfig(serverPropsFilePath);
        CompletableFuture<Map<Integer, RaftConfig.AddressSpec>> controllerQuorumVotersFuture = CompletableFuture.completedFuture(RaftConfig.parseVoterConnections(config.quorumVoters()));

        KafkaRaftManager<ApiMessageAndVersion> kraftManager = new KafkaRaftManager<>(
                metaProps,
                config,
                new MetadataRecordSerde(),
                createMetadataTopicPartition(),
                Uuid.METADATA_TOPIC_ID,
                Time.SYSTEM,
                new Metrics(),
                Option.empty(),
                controllerQuorumVotersFuture
        );

        RaftClient.Listener<ApiMessageAndVersion> simpleListener = new PrintMetaLogListener();
        kraftManager.register(simpleListener);

        LOG.info("Starting KRaft Manager");
        kraftManager.startup();

        while(true) {
            LOG.info("Polling");
            try (KafkaRaftClient<ApiMessageAndVersion> client = kraftManager.client()) {
                client.poll();
            }
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        // Args:
        // 1 - The filepath to the server.properties file
        // 2 - The KRaft cluster id

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");

        runKRaftManager(args[0], args[1]);

    }

}
