public class MasterServerState extends ServerState{
    private String replicationId;
    private long offset;


    public MasterServerState(String replicationId, long offset) {
        super("master");
        this.replicationId=replicationId;
        this.offset=offset;
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getOffset() {
        return offset;
    }
}
