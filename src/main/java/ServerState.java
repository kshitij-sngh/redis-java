import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServerState {
    private int port = Constants.DEFAULT_PORT;
    private String replicationRole="master";

    //Master
    private String masterReplicationId;
    private long masterReplicationOffset;
    private final List<OutputStream> masterReplicationSlaves =new CopyOnWriteArrayList<>();;

    //Slave
    private String masterHost;
    private int masterPort;
    private long replicaOffset=0;

    public boolean isMaster()
    {
        return "master".equals(replicationRole);
    }

    public long getReplicaOffset() {
        return replicaOffset;
    }

    public void setReplicaOffset(long replicaOffset) {
        this.replicaOffset = replicaOffset;
    }

    public List<OutputStream> getMasterReplicationSlaves() {
        return masterReplicationSlaves;
    }

    public void masterReplicationSlavesAddSlave(OutputStream outputStream)
    {
        masterReplicationSlaves.add(outputStream);
    }
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getReplicationRole() {
        return replicationRole;
    }

    public void setReplicationRole(String replicationRole) {
        this.replicationRole = replicationRole;
    }

    public String getMasterReplicationId() {
        return masterReplicationId;
    }

    public void setMasterReplicationId(String masterReplicationId) {
        this.masterReplicationId = masterReplicationId;
    }

    public long getMasterReplicationOffset() {
        return masterReplicationOffset;
    }

    public void setMasterReplicationOffset(long masterReplicationOffset) {
        this.masterReplicationOffset = masterReplicationOffset;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }
}
