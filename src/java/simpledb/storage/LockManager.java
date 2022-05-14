package simpledb.storage;

import simpledb.common.Database;
import java.util.concurrent.locks;

public class LockManager {

    public enum LockMode {
        SHARED, EXCLUSIVE
    }

    public enum LockStatus {
        WAITING, CONVERTING, GRANTED
    }
    
    private class LockRequest {
        public TransactionId tid;
        public LockMode mode;
        public PageId pid;
        public LockStatus status = LockStatus.WAITING;
        public LockMode convertMode;

        public LockRequest(TransactionId tid, LockMode mode, PageId pid) {
            this.tid = tid;
            this.mode = mode;
            this.pid = pid;
        }
    }

    private class LockQueue {
        public List<LockRequest> lockRequests = new ArrayList<LockRequest>();
        public final Lock lock = new ReentrantLock();
        public final Condition notify = lock.newCondition();
    }
    
    private ConcurrentHashMap<PageId, LockQueue> lockTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, Set<PageId>> txnTable = new ConcurrentHashMap<>();

    LockManager() {
    }

    public void lockPage(TransactionId tid, PageId pid, LockMode mode) {
        LockRequest lockReq = new LockRequest(tid, mode, pid);
        LockQueue lockQueue = lockTable.computeIfAbsent(pid, (key) -> new LockQueue());
        lockQueue.lock.lock();
        try {
            for (;;) {
                LockMode grantedMode = null;
                boolean allGranted = true;
                for (LockRequest lr : lockQueue.lockRequests) {
                    if (lockReq.tid.equals(lr.tid)) {
                        if (lockReq.mode == EXCLUSIVE && lr.mode == SHARED) {
                            if (lr.status = GRANTED) {
                                lr.convertMode = lockReq.mode;
                                lr.status = CONVERTING;
                            } else {
                                // lr.status == WAITING
                                lr.mode = lockReq.mode;
                            }
                        }
                        switch (lr.status) {
                        case WAITING:
                            if (isCompatible(grantedMode, lr.mode)) {
                                lr.status = GRANTED;
                            }
                            break;
                        case CONVERTING:
                            if (isCompatible(grantedMode, lr.convertMode)) {
                                lr.mode = lr.convertMode;
                                lr.convertMode = null;
                                lr.status = GRANTED;
                            }
                            break;
                        }
                        if (lr.status == GRANTED) {
                            return;
                        }
                        break;
                    } else {
                        if (lr.status == GRANTED || lr.status == CONVERTING) {
                            grantedMode = maxMode(grantedMode, lr.mode);
                        } else {
                            allGranted = false;
                            break;
                        }
                    }
                }
                if (allGranted && isCompatible(grantedMode, lockReq.mode)) {
                    lockReq.status = GRANTED;
                    lockQueue.add(lockReq);
                    return;
                }
                lockQueue.append(lockReq);
                lockQueue.wait();
            }
        } finally {
            lockQueue.lock.unlock();
        }
        List<PageId> list = txnTable.computeIfAbsent(tid, (key) -> new ArrayList<PageId>());
        list.add(pid);
    }

    public void unlockPage(TransactionId tid, PageId pid) {
        LockQueue lockQueue = lockTable.get(pid);
        if (lockQueue == null) {
            return;
        }
        lockQueue.lock.lock();
        try {
            for (LockRequest lr : lockQueue.lockRequests) {
                if (tid.equals(lr.tid)) {
                    if (lr.status != GRANTED) {
                        throw new IllegalArgumentException("can't unlock ungranted lock");
                    }
                    lockQueue.remove(lr);
                    lockQueue.notify.signal();
                    return;
                }
            }
        } finally {
            lockQueue.lock.unlock();
        }
        Set<PageId> set = txnTable.get(tid);
        if (set != null) {
            set.remove(pid);
        }
    }

    void unlockPages(TransactionId tid) {
        Set<PageId> set = txnTable.get(tid);
        if (set == null) {
            return;
        }
        for (PageId pid : set) {
            unlockPage(tid, pid);
        }
    }

    bool isLocked(TransactionId tid, PageId pid) {
        LockQueue lockQueue = lockTable.get(pid);
        if (lockQueue == null) {
            return false;
        }
        lockQueue.lock.lock();
        try {
            for (LockRequest lr : lockQueue.lockRequests) {
                if (tid.equals(lr.tid)) {
                    if (lr.status == GRANTED) {
                        return true;
                    }
                    return false;
                }
            }
            return false;
        } finally {
            lockQueue.lock.unlock();
        }
    }

    private LockMode maxMode(LockMode a, LockMode b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a == SHARED) {
            return b;
        }
        if (b == SHARED) {
            return a;
        }
        return a;
    }

    private boolean isCompatible(LockMode a, LockMode b) {
        if (a == EXCLUSIVE || b == EXCLUSIVE) {
            return false;
        }
        return true;
    }
}
