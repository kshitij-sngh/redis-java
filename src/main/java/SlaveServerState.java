public class SlaveServerState extends ServerState{
    private final String replicaOf;

    public SlaveServerState(String replicaOf) {
        super("slave");
        this.replicaOf=replicaOf;
    }

    public String getReplicaOf() {
        return replicaOf;
    }
}
