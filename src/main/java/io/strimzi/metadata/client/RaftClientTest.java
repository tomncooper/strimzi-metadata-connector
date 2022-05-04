package io.strimzi.metadata.client;

import kafka.raft.KafkaMetadataLog;
import kafka.raft.KafkaNetworkChannel;
import kafka.raft.MetadataLogConfig;
import kafka.raft.TimingWheelExpirationService;
import kafka.server.KafkaConfig;
import kafka.utils.KafkaScheduler;
import kafka.utils.timer.SystemTimer;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.*;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.OptionalInt;
import java.util.Properties;

public class RaftClientTest {


    public static File createDataDir(TopicPartition topicPartition, String logDir) {

        // The methods needed to get the log dir name do not seem to be exported to maven central.
        // So we just replicate what the kafka.log.LocalLog.logDirName method does.
        String logDirName = topicPartition.topic() + "-" + topicPartition.partition();

        File parentLogDir = new File(logDir);

        // Now we replicate what kafka.raft.KafkaRaftManager#createLogDirectory does.
        String parentLogDirPath = parentLogDir.getAbsolutePath();
        return new File(parentLogDirPath, logDirName);
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

    /**
     * This is a copy of the kafka.raft.KafkaRaftManager#buildNetworkClient method (which was originally written in Scala).
     *
     * @param config
     * @param time
     * @param logContext
     * @param metrics
     * @return A configured Kafka network channel instance.
     */
    public static KafkaNetworkChannel createNetworkChannel(KafkaConfig config, Time time, LogContext logContext, Metrics metrics){

        ListenerName controllerListenerName = new ListenerName(config.controllerListenerNames().head());
        SecurityProtocol controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap().getOrElse(
                controllerListenerName,
                () -> SecurityProtocol.forName(controllerListenerName.value()));
        ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
                controllerSecurityProtocol,
                JaasContext.Type.SERVER,
                config,
                controllerListenerName,
                config.saslMechanismControllerProtocol(),
                time,
                config.saslInterBrokerHandshakeRequestEnable(),
                logContext);

        String metricGroupPrefix = "raft-channel";
        boolean collectPerConnectionMetrics = false;

        Selector selector = new Selector(
                NetworkReceive.UNLIMITED,
                config.connectionsMaxIdleMs(),
                metrics,
                time,
                metricGroupPrefix,
                new HashMap<String, String>(),
                collectPerConnectionMetrics,
                channelBuilder,
                logContext);

        String clientId = "raft-client-metadata-watcher";
        int maxInflightRequestsPerConnection = 1;
        int reconnectBackoffMs = 50;
        int reconnectBackoffMsMs = 500;
        boolean discoverBrokerVersions = true;

       NetworkClient netClient = new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                clientId,
                maxInflightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMsMs,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.socketReceiveBufferBytes(),
                config.quorumRequestTimeoutMs(),
                config.connectionSetupTimeoutMs(),
                config.connectionSetupTimeoutMaxMs(),
                time,
                discoverBrokerVersions,
                new ApiVersions(),
                logContext
       );

        return new KafkaNetworkChannel(time, netClient, config.quorumRequestTimeoutMs(), "kafka-raft");
    }



    public static KafkaRaftClient<ApiMessageAndVersion> createKafkaRaftClient(
            Time time,
            KafkaScheduler scheduler,
            KafkaConfig config,
            String clusterId) {

        TopicPartition metadataTopicPartition = createMetadataTopicPartition();
        String logDir = config.getString("log.dirs");
        File dataDir = createDataDir(metadataTopicPartition, logDir);

        RecordSerde<ApiMessageAndVersion> recordSerde = new MetadataRecordSerde();
        LogContext logContext = new LogContext("[RaftWatcher] ");
        Metrics metrics = new Metrics();
        NetworkChannel channel = createNetworkChannel(config, time, logContext, metrics);
        ReplicatedLog log = createMetadataLog(metadataTopicPartition, dataDir, time, scheduler, config);
        QuorumStateStore quorumStateStore = new FileBasedStateStore(new File(dataDir, "quorum-state"));
        SystemTimer expirationTimer = new SystemTimer("raft-expiration-executor", 1, 20, Time.SYSTEM.hiResClockMs());
        TimingWheelExpirationService expirationService = new TimingWheelExpirationService(expirationTimer);
        RaftConfig raftConfig = new RaftConfig(config);

        return new KafkaRaftClient<>(
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
    }

    public static void main(String[] args) throws IOException {

        // Args:
        // 1 - The filepath to the server.properties file
        // 2 - The KRaft cluster id

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
        Logger LOG = LoggerFactory.getLogger(RaftClientTest.class);

        LOG.info("Creating Kafka Scheduler");
        KafkaScheduler scheduler = new KafkaScheduler(1, "kafka-raft" + "-scheduler", true);
        scheduler.startup();

        String serverPropsFilePath = args[0];
        LOG.info("Reading config from: " + serverPropsFilePath);
        Properties props = Utils.loadProps(serverPropsFilePath);
        KafkaConfig config = KafkaConfig.fromProps(props,false);
        String clusterID = args[1];

        LOG.info("Creating Kafka Raft Client");
        KafkaRaftClient<ApiMessageAndVersion> raftClient = createKafkaRaftClient(Time.SYSTEM, scheduler, config, clusterID);

        LOG.info("Initializing Kafka Raft Client");
        raftClient.initialize();

        LOG.info("Registering log message listener");
        RaftClient.Listener<ApiMessageAndVersion> simpleListener = new SimpleMetaLogListener();
        raftClient.register(simpleListener);

        LOG.info("Leader and Epoch: " + raftClient.leaderAndEpoch());

    }

}
