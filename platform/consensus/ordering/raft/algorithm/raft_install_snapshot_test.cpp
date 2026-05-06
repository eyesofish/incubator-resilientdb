#include "platform/consensus/ordering/raft/algorithm/raft_tests.h"

namespace resdb {
namespace raft {

// Test 1: Follower rejects InstallSnapshot with stale term.
TEST_F(RaftTest, FollowerRejectsStaleTermInstallSnapshot) {
  raft_->SetStateForTest({
      .currentTerm = 5,
      .role = Role::FOLLOWER,
  });

  auto req = std::make_unique<InstallSnapshotRequest>();
  req->set_term(3);  // stale
  req->set_leader_id(2);
  req->set_last_included_index(10);

  EXPECT_CALL(mock_call, Call(_, _, _))
      .WillOnce(::testing::Invoke(
          [](int type, const google::protobuf::Message& msg, int node_id) {
            EXPECT_EQ(type, MessageType::InstallSnapshotResponseMsg);
            const auto& resp =
                dynamic_cast<const InstallSnapshotResponse&>(msg);
            EXPECT_FALSE(resp.success());
            EXPECT_EQ(resp.term(), 5);
            return 0;
          }));

  raft_->ReceiveInstallSnapshot(std::move(req));
}

// Test 2: Follower receives a single-chunk InstallSnapshot and applies it.
TEST_F(RaftTest, FollowerReceivesSingleChunkInstallSnapshot) {
  raft_->SetStateForTest({
      .currentTerm = 2,
      .role = Role::FOLLOWER,
  });

  bool restore_called = false;
  raft_->SetSnapshotCallbacks(
      nullptr,
      [&restore_called](const std::string& data) -> bool {
        restore_called = true;
        EXPECT_EQ(data, "snapshot_payload");
        return true;
      });

  std::string payload = "snapshot_payload";
  std::string hash = SignatureVerifier::CalculateHash(payload);

  auto req = std::make_unique<InstallSnapshotRequest>();
  req->set_term(2);
  req->set_leader_id(2);
  req->set_last_included_index(10);
  req->set_last_included_term(2);
  req->set_chunk_index(0);
  req->set_data(payload);
  req->set_last_chunk(true);
  req->set_state_hash(hash);

  EXPECT_CALL(mock_call, Call(_, _, _))
      .WillOnce(::testing::Invoke(
          [](int type, const google::protobuf::Message& msg, int node_id) {
            EXPECT_EQ(type, MessageType::InstallSnapshotResponseMsg);
            const auto& resp =
                dynamic_cast<const InstallSnapshotResponse&>(msg);
            EXPECT_TRUE(resp.success());
            EXPECT_EQ(resp.applied_index(), 10);
            return 0;
          }));

  EXPECT_CALL(*recovery_, WriteSnapshotData(payload, 10, 2)).Times(1);

  raft_->ReceiveInstallSnapshot(std::move(req));
  EXPECT_TRUE(restore_called);
}

// Test 3: Follower accumulates multiple chunks correctly.
TEST_F(RaftTest, FollowerAccumulatesMultiChunkInstallSnapshot) {
  raft_->SetStateForTest({
      .currentTerm = 1,
      .role = Role::FOLLOWER,
  });

  std::string chunk0 = "AAAA";  // 4 bytes
  std::string chunk1 = "BBBB";  // 4 bytes
  std::string chunk2 = "CCCC";  // 4 bytes
  std::string full = chunk0 + chunk1 + chunk2;
  std::string hash = SignatureVerifier::CalculateHash(full);

  std::string restored_data;
  raft_->SetSnapshotCallbacks(
      nullptr,
      [&restored_data](const std::string& data) -> bool {
        restored_data = data;
        return true;
      });

  // Chunk 0 (not last).
  {
    auto req = std::make_unique<InstallSnapshotRequest>();
    req->set_term(1);
    req->set_leader_id(2);
    req->set_last_included_index(5);
    req->set_last_included_term(1);
    req->set_chunk_index(0);
    req->set_data(chunk0);
    req->set_last_chunk(false);
    req->set_state_hash(hash);

    EXPECT_CALL(mock_call, Call(_, _, _))
        .WillOnce(::testing::Invoke(
            [](int type, const google::protobuf::Message& msg, int node_id) {
              const auto& resp =
                  dynamic_cast<const InstallSnapshotResponse&>(msg);
              EXPECT_TRUE(resp.success());
              return 0;
            }));

    raft_->ReceiveInstallSnapshot(std::move(req));
    EXPECT_TRUE(restored_data.empty());  // Not yet applied.
  }

  // Chunk 1 (not last).
  {
    auto req = std::make_unique<InstallSnapshotRequest>();
    req->set_term(1);
    req->set_leader_id(2);
    req->set_last_included_index(5);
    req->set_last_included_term(1);
    req->set_chunk_index(1);
    req->set_data(chunk1);
    req->set_last_chunk(false);
    req->set_state_hash(hash);

    EXPECT_CALL(mock_call, Call(_, _, _))
        .WillOnce(::testing::Invoke(
            [](int type, const google::protobuf::Message& msg, int node_id) {
              const auto& resp =
                  dynamic_cast<const InstallSnapshotResponse&>(msg);
              EXPECT_TRUE(resp.success());
              return 0;
            }));

    raft_->ReceiveInstallSnapshot(std::move(req));
    EXPECT_TRUE(restored_data.empty());
  }

  // Chunk 2 (last chunk).
  {
    auto req = std::make_unique<InstallSnapshotRequest>();
    req->set_term(1);
    req->set_leader_id(2);
    req->set_last_included_index(5);
    req->set_last_included_term(1);
    req->set_chunk_index(2);
    req->set_data(chunk2);
    req->set_last_chunk(true);
    req->set_state_hash(hash);

    EXPECT_CALL(mock_call, Call(_, _, _))
        .WillOnce(::testing::Invoke(
            [](int type, const google::protobuf::Message& msg, int node_id) {
              const auto& resp =
                  dynamic_cast<const InstallSnapshotResponse&>(msg);
              EXPECT_TRUE(resp.success());
              EXPECT_EQ(resp.applied_index(), 5);
              return 0;
            }));

    EXPECT_CALL(*recovery_, WriteSnapshotData(full, 5, 1)).Times(1);

    raft_->ReceiveInstallSnapshot(std::move(req));
    EXPECT_EQ(restored_data, full);
  }
}

// Test 4: Follower rejects snapshot with hash mismatch.
TEST_F(RaftTest, FollowerRejectsHashMismatch) {
  raft_->SetStateForTest({
      .currentTerm = 1,
      .role = Role::FOLLOWER,
  });

  raft_->SetSnapshotCallbacks(nullptr, nullptr);

  auto req = std::make_unique<InstallSnapshotRequest>();
  req->set_term(1);
  req->set_leader_id(2);
  req->set_last_included_index(10);
  req->set_last_included_term(1);
  req->set_chunk_index(0);
  req->set_data("payload");
  req->set_last_chunk(true);
  req->set_state_hash("wrong_hash_0123456789abcdef01234567");

  EXPECT_CALL(*recovery_, WriteSnapshotData(_, _, _)).Times(0);

  EXPECT_CALL(mock_call, Call(_, _, _))
      .WillOnce(::testing::Invoke(
          [](int type, const google::protobuf::Message& msg, int node_id) {
            const auto& resp =
                dynamic_cast<const InstallSnapshotResponse&>(msg);
            EXPECT_FALSE(resp.success());
            return 0;
          }));

  raft_->ReceiveInstallSnapshot(std::move(req));
}

// Test 5: Follower skips older snapshot (idempotency).
TEST_F(RaftTest, FollowerSkipsOlderSnapshot) {
  raft_->SetStateForTest({
      .currentTerm = 3,
      .role = Role::FOLLOWER,
  });
  raft_->SetSnapshotLastIndexAndTerm(20, 2, false);

  raft_->SetSnapshotCallbacks(nullptr, nullptr);

  auto req = std::make_unique<InstallSnapshotRequest>();
  req->set_term(3);
  req->set_leader_id(2);
  req->set_last_included_index(10);  // older than 20
  req->set_last_included_term(1);

  EXPECT_CALL(*recovery_, WriteSnapshotData(_, _, _)).Times(0);

  EXPECT_CALL(mock_call, Call(_, _, _))
      .WillOnce(::testing::Invoke(
          [](int type, const google::protobuf::Message& msg, int node_id) {
            const auto& resp =
                dynamic_cast<const InstallSnapshotResponse&>(msg);
            EXPECT_TRUE(resp.success());
            EXPECT_EQ(resp.applied_index(), 20);
            return 0;
          }));

  raft_->ReceiveInstallSnapshot(std::move(req));
}

// Test 6: Leader updates nextIndex/matchIndex on InstallSnapshotResponse success.
TEST_F(RaftTest, LeaderUpdatesTrackingOnInstallSnapshotResponseSuccess) {
  raft_->SetStateForTest({
      .currentTerm = 3,
      .role = Role::LEADER,
      .nextIndex = std::vector<uint64_t>{1, 1, 1, 1, 1},
      .matchIndex = std::vector<uint64_t>{1, 0, 0, 0, 0},
  });

  auto resp = std::make_unique<InstallSnapshotResponse>();
  resp->set_term(3);
  resp->set_success(true);
  resp->set_responder_id(2);
  resp->set_applied_index(10);

  raft_->ReceiveInstallSnapshotResponse(std::move(resp));

  EXPECT_EQ(raft_->GetNextIndex()[2], 11);
  EXPECT_EQ(raft_->GetMatchIndex()[2], 10);
}

// Test 7: Leader demotes on higher term InstallSnapshotResponse.
TEST_F(RaftTest, LeaderDemotesOnHigherTermInstallSnapshotResponse) {
  raft_->SetStateForTest({
      .currentTerm = 3,
      .role = Role::LEADER,
  });

  EXPECT_CALL(*leader_election_manager_, OnAeBroadcast()).Times(0);

  auto resp = std::make_unique<InstallSnapshotResponse>();
  resp->set_term(5);  // higher than currentTerm
  resp->set_success(true);
  resp->set_responder_id(2);

  EXPECT_CALL(*leader_election_manager_, OnRoleChange()).Times(1);

  raft_->ReceiveInstallSnapshotResponse(std::move(resp));

  EXPECT_NE(raft_->GetRoleSnapshot(), Role::LEADER);
}

// Test 8: Leader takes snapshot, dumps state, and truncates log prefix.
TEST_F(RaftTest, LeaderTakesSnapshotAndTruncatesLog) {
  // Build a log where the sentinel at index 0 has the term that will
  // match the snapshot — TruncatePrefixLocked asserts this invariant.
  std::vector<LogEntry> log;
  // Sentinel at index 0: represents last snapshot state (term=3 for this test).
  LogEntry sentinel;
  sentinel.entry.set_term(3);
  sentinel.entry.set_command("SENTINEL");
  log.push_back(sentinel);
  // Append regular entries at indices 1-4.
  for (auto& e : CreateLogEntries({{1, "txn1"}, {1, "txn2"}, {2, "txn3"},
                                     {3, "txn4"}},
                                   false)) {
    log.push_back(e);
  }

  raft_->SetStateForTest({
      .currentTerm = 3,
      .commitIndex = 4,
      .lastCommitted = 4,
      .role = Role::LEADER,
      .log = log,
  });

  std::string dump_output = "snapshot_state_data";

  bool dump_called = false;
  raft_->SetSnapshotCallbacks(
      [&dump_called, dump_output]() -> std::string {
        dump_called = true;
        return dump_output;
      },
      nullptr);

  EXPECT_CALL(*recovery_, WriteSnapshotData(dump_output, 4, 3)).Times(1);

  raft_->TakeSnapshot(4);

  EXPECT_TRUE(dump_called);
  // After TruncatePrefix(4), the sentinel (now at index 4's entry) + entries
  // after index 4 should remain. Since we had 5 entries (sentinel + 4 items)
  // and truncated at logical index 4, only the sentinel remains = 1 entry.
  EXPECT_EQ(raft_->GetLogSize(), 1);
}

// Test 9: SendInstallSnapshot streams chunks to the follower.
TEST_F(RaftTest, SendInstallSnapshotSendsChunks) {
  raft_->SetStateForTest({
      .currentTerm = 2,
      .role = Role::LEADER,
  });
  raft_->SetSnapshotLastIndexAndTerm(5, 1, false);

  // Create data just over one chunk to test chunking.
  std::string snap_data(70000, 'X');  // ~70KB → 2 chunks with 64KB default
  std::string hash = SignatureVerifier::CalculateHash(snap_data);

  EXPECT_CALL(*recovery_, ReadSnapshotData())
      .WillOnce(::testing::Return(snap_data));

  // Expect 2 chunks: chunk 0 (64KB, not last), chunk 1 (remaining, last).
  EXPECT_CALL(mock_call, Call(_, _, _))
      .WillOnce(::testing::Invoke(
          [hash](int type, const google::protobuf::Message& msg, int node_id) {
            EXPECT_EQ(type, MessageType::InstallSnapshotMsg);
            const auto& req =
                dynamic_cast<const InstallSnapshotRequest&>(msg);
            EXPECT_EQ(req.chunk_index(), 0);
            EXPECT_FALSE(req.last_chunk());
            EXPECT_EQ(req.state_hash(), hash);
            EXPECT_EQ(req.data().size(), 65536);
            return 0;
          }))
      .WillOnce(::testing::Invoke(
          [hash](int type, const google::protobuf::Message& msg, int node_id) {
            EXPECT_EQ(type, MessageType::InstallSnapshotMsg);
            const auto& req =
                dynamic_cast<const InstallSnapshotRequest&>(msg);
            EXPECT_EQ(req.chunk_index(), 1);
            EXPECT_TRUE(req.last_chunk());
            EXPECT_EQ(req.state_hash(), hash);
            EXPECT_EQ(req.data().size(), 70000 - 65536);
            return 0;
          }));

  raft_->SendInstallSnapshot(2);
}

}  // namespace raft
}  // namespace resdb
