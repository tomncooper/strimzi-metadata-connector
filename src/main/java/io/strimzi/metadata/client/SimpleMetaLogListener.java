package io.strimzi.metadata.client;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotReader;

import java.util.List;

class SimpleMetaLogListener implements RaftClient.Listener<ApiMessageAndVersion> {

    @Override
    public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {

        try (reader) {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                long offset = batch.lastOffset();
                int epoch = batch.epoch();
                List<ApiMessageAndVersion> messages = batch.records();

                System.out.println("We got a Commit Message!");
                System.out.println("Offset: " + offset + ", Epoch: " + epoch);
                messages.forEach(message -> System.out.println(message.message()));
            }
        }

        throw new RuntimeException("We got a commit message!");
    }

    @Override
    public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        try (reader) {
            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                long offset = batch.lastOffset();
                List<ApiMessageAndVersion> messages = batch.records();

                System.out.println("We got a Snapshot Message!");
                System.out.println("Last offset: " + offset);
                messages.forEach(message -> System.out.println(message.message()));
            }
        }
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch newLeader) {
        System.out.println("We have been told about a new leader (spoiler: it is definitely not us)");
        System.out.println("New Leader: " + newLeader.leaderId());
    }

    @Override
    public void beginShutdown() {
        System.out.println("Shutting down simple meta log listener");
    }
}
