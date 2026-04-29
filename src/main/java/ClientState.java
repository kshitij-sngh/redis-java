import java.util.ArrayList;
import java.util.List;

public class ClientState {
    private boolean isDirty = false;
    private TransactionStatus transactionStatus = TransactionStatus.NO;
    private List<String> watchedKeys = new ArrayList<>();

    public boolean isDirty() {
        return isDirty;
    }

    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    public List<String> getWatchedKeys() {
        return watchedKeys;
    }

}
