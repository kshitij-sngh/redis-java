public class ServerState {
    private String replicationRole = "master";
    private String replicaOf = "";

    public ServerState(String replicationRole, String replicaOf) {
        this.replicationRole = replicationRole;
        this.replicaOf = replicaOf;
    }

    public String getReplicationRole() {
        return replicationRole;
    }

    public String getReplicaOf() {
        return replicaOf;
    }
}
