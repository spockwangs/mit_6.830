package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;

import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;

public class LockManager {

    public enum LockMode {
        SHARED, EXCLUSIVE
    }

    public enum LockStatus {
        WAITING, CONVERTING, GRANTED
    }
    
    private class LockRequest {
        public final TransactionId tid;
        public LockMode mode;
        public final PageId pid;
        public LockStatus status = LockStatus.WAITING;
        public LockMode convertMode;
        public Condition notify;

        public LockRequest(TransactionId tid, LockMode mode, PageId pid) {
            this.tid = tid;
            this.mode = mode;
            this.pid = pid;
        }
    }

    private class LockQueue {
        public List<LockRequest> lockRequests = new ArrayList<LockRequest>();
        public final Lock lock = new ReentrantLock();
    }
    
    private class TransactionControlBlock {
        public List<LockRequest> lockRequests = new ArrayList<LockRequest>();
        public LockRequest wait = null;
        public TransactionControlBlock cycle = null;
    }
    
    private ConcurrentHashMap<PageId, LockQueue> lockTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, TransactionControlBlock> txnTable = new ConcurrentHashMap<>();

    public LockManager() {
    }

    public void lockPage(TransactionId tid, PageId pid, LockMode mode) {
        lockPage(tid, pid, mode, true);
    }

    public boolean tryLockPage(TransactionId tid, PageId pid, LockMode mode) {
        return lockPage(tid, pid, mode, false);
    }
    
    private boolean lockPage(TransactionId tid, PageId pid, LockMode mode, boolean wait) {
        LockQueue lockQueue = lockTable.computeIfAbsent(pid, (key) -> new LockQueue());
        lockQueue.lock.lock();
        try {
            LockMode maxGrantedMode = null;
            boolean waiting = false;
            LockRequest lockReq = null;
            for (LockRequest lr : lockQueue.lockRequests) {
                if (tid.equals(lr.tid)) {
                    lockReq = lr;
                } else {
                    switch (lr.status) {
                    case WAITING:
                        waiting = true;
                        break;
                    case GRANTED:
                        maxGrantedMode = maxMode(maxGrantedMode, lr.mode);
                        break;
                    case CONVERTING:
                        waiting = true;
                        maxGrantedMode = maxMode(maxGrantedMode, lr.mode);
                        break;
                    }
                }
            }
            if (lockReq == null) {
                lockReq = new LockRequest(tid, mode, pid);
                lockReq.notify = lockQueue.lock.newCondition();
                if (!waiting && isCompatible(maxGrantedMode, lockReq.mode)) {
                    lockReq.status = LockStatus.GRANTED;
                }
                if (wait) {
                    lockQueue.lockRequests.add(lockReq);
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock());
                    synchronized(tcb) {
                        if (lockReq.status != LockStatus.GRANTED) {
                            tcb.wait = lockReq;
                        } else {
                            tcb.wait = null;
                        }
                        tcb.lockRequests.add(lockReq);
                    }
                    while (lockReq.status != LockStatus.GRANTED) {
                        lockReq.notify.awaitUninterruptibly();
                    }
                    synchronized(tcb) {
                        tcb.wait = null;
                    }
                } else if (lockReq.status == LockStatus.GRANTED) {
                    lockQueue.lockRequests.add(lockReq);                        
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock());
                    synchronized(tcb) {
                        tcb.wait = null;
                        tcb.lockRequests.add(lockReq);
                    }
                } else {
                    return false;
                }
            } else {
                // Conversion case
                if (mode == LockMode.EXCLUSIVE && lockReq.mode == LockMode.SHARED) {
                    if (lockReq.status == LockStatus.GRANTED) {
                        lockReq.convertMode = mode;
                        lockReq.status = LockStatus.CONVERTING;
                    } else {
                        lockReq.mode = mode;
                    }
                }
                switch (lockReq.status) {
                case WAITING:
                    throw new IllegalArgumentException("bad lock status");
                case CONVERTING:
                    if (isCompatible(lockReq.convertMode, maxGrantedMode)) {
                        lockReq.status = LockStatus.GRANTED;
                        lockReq.mode = lockReq.convertMode;
                        lockReq.convertMode = null;
                    }
                    break;
                }
                if (wait) {
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock());
                    synchronized(tcb) {
                        if (lockReq.status == LockStatus.GRANTED) {
                            tcb.wait = null;
                        } else {
                            tcb.wait = lockReq;
                        }
                    }
                    while (lockReq.status != LockStatus.GRANTED) {

                        lockReq.notify.awaitUninterruptibly();
                    }
                } else if (lockReq.status == LockStatus.GRANTED) {
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock());
                    synchronized(tcb) {
                        tcb.wait = null;
                    }
                } else {
                    // Clear the conversion info.
                    lockReq.status = LockStatus.WAITING;
                    lockReq.convertMode = null;
                    return false;
                }
            }
        } finally {
            lockQueue.lock.unlock();
        }
        return true;
    }

    public void unlockPage(TransactionId tid, PageId pid) {
        LockQueue lockQueue = lockTable.get(pid);
        if (lockQueue == null) {
            return;
        }
        lockQueue.lock.lock();
        try {
            LockRequest lockReq = null;
            LockMode maxGrantedMode = null;
            boolean conversionWaiting = false;
            for (LockRequest lr : lockQueue.lockRequests) {
                if (tid.equals(lr.tid)) {
                    if (lr.status != LockStatus.GRANTED) {
                        throw new IllegalArgumentException("can't unlock ungranted lock");
                    }
                    lockReq = lr;
                } else if (lr.status == LockStatus.GRANTED) {
                    maxGrantedMode = maxMode(maxGrantedMode, lr.mode);
                } else if (lr.status == LockStatus.WAITING) {
                    if (!conversionWaiting && isCompatible(lr.mode, maxGrantedMode)) {
                        lr.status = LockStatus.GRANTED;
                        lr.notify.signal();
                        maxGrantedMode = maxMode(maxGrantedMode, lr.mode);
                    } else {
                        break;
                    }
                } else {
                    // Conversion case
                    LockMode maxMode = null;
                    for (LockRequest l : lockQueue.lockRequests) {
                        if (tid.equals(l.tid) || l == lr) {
                            continue;
                        }
                        if (l.status == LockStatus.GRANTED || l.status == LockStatus.CONVERTING) {
                            maxMode = maxMode(maxMode, l.mode);
                        } else {
                            break;
                        }
                    }
                    if (isCompatible(lr.convertMode, maxMode)) {
                        lr.status = LockStatus.GRANTED;
                        lr.mode = lr.convertMode;
                        lr.convertMode = null;
                        lr.notify.signal();
                        maxGrantedMode = maxMode(maxGrantedMode, lr.mode);
                    } else {
                        conversionWaiting = true;
                    }
                }
            }
            lockQueue.lockRequests.remove(lockReq);
            TransactionControlBlock tcb = txnTable.get(tid);
            synchronized(tcb) {
                tcb.lockRequests.remove(lockReq);
            }
        } finally {
            lockQueue.lock.unlock();
        }
    }

    public void unlockPages(TransactionId tid) {
        TransactionControlBlock tcb = txnTable.get(tid);
        if (tcb == null) {
            return;
        }
        HashSet<PageId> set = new HashSet<>();
        synchronized(tcb) {
            for (LockRequest lr : tcb.lockRequests) {
                set.add(lr.pid);
            }
        }
        for (PageId pid : set) {
            unlockPage(tid, pid);
        }
        txnTable.remove(tid);
    }

    public boolean isLocked(TransactionId tid, PageId pid) {
        LockQueue lockQueue = lockTable.get(pid);
        if (lockQueue == null) {
            return false;
        }
        lockQueue.lock.lock();
        try {
            for (LockRequest lr : lockQueue.lockRequests) {
                if (lr.tid.equals(tid)) {
                    if (lr.status == LockStatus.GRANTED || lr.status == LockStatus.CONVERTING) {
                        return true;
                    } else {
                        return false;
                    }
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
        if (a == LockMode.SHARED) {
            return b;
        }
        if (b == LockMode.SHARED) {
            return a;
        }
        return a;
    }

    private boolean isCompatible(LockMode a, LockMode b) {
        if (a == null || b == null) {
            return true;
        }
        if (a == LockMode.EXCLUSIVE || b == LockMode.EXCLUSIVE) {
            return false;
        }
        return true;
    }

    /*
    private void detectDeadlock() {
        HashMap<TransactionId, HashSet<TransactionId>> graph = new HashMap<>();
        for (Map.Entry<PageId, LockQueue> e : lockTable) {
            LockQueue lockQueue = e.getValue();
            lockQueue.lock.lock();
            try {
                HashSet<TransactionId> waited = new HashSet<>();
                HashSet<TransactionId> waiters = new HashSet<>();
                for (LockRequest lr : lockQueue.lockRequests) {
                    if (lr.status == LockMode.GRANTED) {
                        waited.add(lr.tid);
                    } else if (lr.status == LockMode.CONVERTING || lr.status == LockMode.WAITING) {
                        waiters.add(lr.tid);
                    }
                }
                for (TransactionId tid : waiters) {
                    HashSet<TransactionId> set = graph.computeIfAbsent(tid, (key) -> new HashSet<TransactionId>());
                    for (TransactionId tid2 : waited) {
                        set.add(tid2);
                    }
                }
            } finally {
                lockQueue.lock.unlock();
            }
        }

        List<TransactionId> cycle = detectCycle(graph);
        if (cycle != null) {
        }
    }
    */
        
}
