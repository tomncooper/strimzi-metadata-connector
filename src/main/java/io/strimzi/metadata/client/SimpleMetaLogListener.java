package io.strimzi.metadata.client;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class SimpleMetaLogListener implements RaftClient.Listener<ApiMessageAndVersion> {

    Logger LOG = LoggerFactory.getLogger(SimpleMetaLogListener.class);

    @Override
    public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {

        try (reader) {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                long offset = batch.lastOffset();
                int epoch = batch.epoch();
                List<ApiMessageAndVersion> messages = batch.records();

                LOG.debug("We got a Commit Message!");
                LOG.debug("Offset: " + offset + ", Epoch: " + epoch);
                messages.forEach(message -> LOG.debug(message.message().toString()));
            }
        }
    }

    @Override
    public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        try (reader) {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                long offset = batch.lastOffset();
                List<ApiMessageAndVersion> messages = batch.records();

                LOG.debug("We got a Snapshot Message!");
                LOG.debug("Last offset: " + offset);
                messages.forEach(message -> LOG.debug(message.message().toString()));
            }
        }
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch newLeader) {
        LOG.debug("We have been told about a new leader (spoiler: it is definitely not us)");
        LOG.debug("New Leader: " + newLeader.leaderId());
    }

    @Override
    public void beginShutdown() {
        LOG.debug("Shutting down simple meta log listener");
    }
}
