package io.strimzi.metadata.client;

import kafka.raft.KafkaMetadataLog;
import kafka.raft.MetadataLogConfig;
import kafka.raft.TimingWheelExpirationService;
import kafka.server.KafkaConfig;
import kafka.utils.KafkaScheduler;
import kafka.utils.timer.SystemTimer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.*;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.io.File;
import java.io.IOException;
import java.util.OptionalInt;
import java.util.Properties;

public class RaftClientTest {

    public static File createDataDir(TopicPartition topicPartition) {

        // The methods needed to get the log dir name do not seem to be exported to maven central.
        // So we just replicate what the kafka.log.LocalLog.logDirName method does.
        String logDirName = topicPartition.topic() + "-" + topicPartition.partition();

        String parentLogDir = "logs";
        File logDir = new File(parentLogDir);

        // Now we replicate what kafka.raft.KafkaRaftManager#createLogDirectory does.
        String logDirPath = logDir.getAbsolutePath();
        return new File(logDirPath, logDirName);
    }

    public static TopicPartition createMetadataTopicPartition(){
        String metadataTopicName = "__cluster_metadata";
        return new TopicPartition(metadataTopicName, 0);
    }

    public static KafkaMetadataLog createMetadataLog(
            TopicPartition metadataTopicPartition,
            File dataDir,
            Time time,
            KafkaScheduler scheduler,
            KafkaConfig config) {

        Uuid metadataTopicId = Uuid.METADATA_TOPIC_ID;

        MetadataLogConfig metadataLogConfig = MetadataLogConfig.apply(config, KafkaRaftClient.MAX_BATCH_SIZE_BYTES, KafkaRaftClient.MAX_FETCH_SIZE_BYTES);

        return KafkaMetadataLog.apply(
                metadataTopicPartition,
                metadataTopicId,
                dataDir,
                time,
                scheduler,
                metadataLogConfig
        );
    }

    public static KafkaRaftClient<ApiMessageAndVersion> createKafkaRaftClient(
            Time time,
            KafkaScheduler scheduler,
            KafkaConfig config,
            String clusterId) {

        TopicPartition metadataTopicPartition = createMetadataTopicPartition();
        File dataDir = createDataDir(metadataTopicPartition);


        RecordSerde<ApiMessageAndVersion> recordSerde = new MetadataRecordSerde();
        // TODO: Figure out how to create one of these
        NetworkChannel channel = null;
        ReplicatedLog log = createMetadataLog(metadataTopicPartition, dataDir, time, scheduler, config);
        QuorumStateStore quorumStateStore = new FileBasedStateStore(new File(dataDir, "quorum-state"));
        Metrics metrics = new Metrics();
        SystemTimer expirationTimer = new SystemTimer("raft-expiration-executor", 1, 20, Time.SYSTEM.hiResClockMs());
        TimingWheelExpirationService expirationService = new TimingWheelExpirationService(expirationTimer);
        LogContext logContext = new LogContext("[RaftWatcher] ");
        RaftConfig raftConfig = new RaftConfig(config);

        KafkaRaftClient<ApiMessageAndVersion> raftClient = new KafkaRaftClient<>(
                recordSerde,
                channel,
                log,
                quorumStateStore,
                time,
                metrics,
                expirationService,
                logContext,
                clusterId,
                OptionalInt.empty(),
                raftConfig
        );

        return raftClient;
    }

    public static void Main(String[] args) throws IOException {

        KafkaScheduler scheduler = new KafkaScheduler(1, "kafka-raft" + "-scheduler", true);
        scheduler.startup();

        String serverPropsFilePath = "server.properties";
        Properties props = Utils.loadProps(serverPropsFilePath);
        KafkaConfig config = KafkaConfig.fromProps(props,false);
        String clusterID = (String) props.get("clusterId");

        KafkaRaftClient<ApiMessageAndVersion> raftClient = createKafkaRaftClient(Time.SYSTEM, scheduler, config, clusterID);

    }

}
