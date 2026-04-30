public class ServerState {
    private final String replicationRole;

    public ServerState(String replicationRole) {
        this.replicationRole = replicationRole;
    }

    public String getReplicationRole() {
        return replicationRole;
    }
}
