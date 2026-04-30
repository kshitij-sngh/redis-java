public class ServerState {
    private String replicationRole = "master";

    public ServerState(String replicationRole) {
        this.replicationRole = replicationRole;
    }

    public String getReplicationRole() {
        return replicationRole;
    }

    public void setReplicationRole(String replicationRole) {
        this.replicationRole = replicationRole;
    }
}
