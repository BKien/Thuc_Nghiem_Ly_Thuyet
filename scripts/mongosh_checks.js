// =============================================================================
// TẬP LỆNH MONGOSH KIỂM TRA TRẠNG THÁI CLUSTER (Mục 10.5 & Phụ lục B)
// Chạy lệnh:
//   mongosh "mongodb://cluster_admin:ClusterAdminSecret2026!@mongo1:27017,mongo2:27017,mongo3:27017/cluster_lab?authSource=admin&replicaSet=rs0" scripts/mongosh_checks.js
// =============================================================================

print("=================================================================");
print("1. KIỂM TRA TRẠNG THÁI THÀNH VIÊN: rs.status()");
print("=================================================================");
const status = rs.status();
printjson({
  set: status.set,
  term: status.term,
  date: status.date,
  members: status.members.map(m => ({
    _id: m._id,
    name: m.name,
    health: m.health,
    state: m.state,
    stateStr: m.stateStr,
    uptime: m.uptime,
    optimeDate: m.optimeDate,
    lastHeartbeat: m.lastHeartbeat,
    syncSourceHost: m.syncSourceHost
  }))
});

print("\n=================================================================");
print("2. KIỂM TRA CẤU HÌNH REPLICA SET: rs.conf()");
print("=================================================================");
const conf = rs.conf();
printjson({
  _id: conf._id,
  version: conf.version,
  writeConcernMajorityJournalDefault: conf.writeConcernMajorityJournalDefault,
  members: conf.members.map(m => ({
    _id: m._id,
    host: m.host,
    votes: m.votes,
    priority: m.priority,
    arbiterOnly: m.arbiterOnly,
    hidden: m.hidden
  })),
  settings: {
    heartbeatIntervalMillis: conf.settings.heartbeatIntervalMillis,
    electionTimeoutMillis: conf.settings.electionTimeoutMillis,
    chainingAllowed: conf.settings.chainingAllowed
  }
});

print("\n=================================================================");
print("3. KIỂM TRA TOPOLOGY VÀ VAI TRÒ NODE HIỆN TẠI: db.hello()");
print("=================================================================");
const hello = db.hello();
printjson({
  isWritablePrimary: hello.isWritablePrimary,
  setName: hello.setName,
  primary: hello.primary,
  hosts: hello.hosts,
  me: hello.me,
  maxBsonObjectSize: hello.maxBsonObjectSize
});

print("\n=================================================================");
print("4. KIỂM TRA CỬA SỔ OPLOG: rs.printReplicationInfo()");
print("=================================================================");
rs.printReplicationInfo();

print("\n=================================================================");
print("5. KIỂM TRA ĐỘ TRỄ SAO CHÉP SECONDARY: rs.printSecondaryReplicationInfo()");
print("=================================================================");
rs.printSecondaryReplicationInfo();
