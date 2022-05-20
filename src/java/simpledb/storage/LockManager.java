package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TransactionAbortedException;

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

    public enum LockClass {
        SHORT, LONG
    }

    private enum LockStatus {
        WAITING, CONVERTING, GRANTED, DENIED
    }
    
    private class LockRequest {
        public final TransactionId tid;
        public LockMode mode;
        public final PageId pid;
        public LockStatus status = LockStatus.WAITING;
        public LockMode convertMode;
        public Condition notify;
        public LockQueue head;
        public LockClass lockClass;
        public int count;

        public LockRequest(TransactionId tid, LockMode mode, PageId pid, Condition notify, LockQueue head, LockClass lockClass) {
            this.tid = tid;
            this.mode = mode;
            this.pid = pid;
            this.notify = notify;
            this.head = head;
            this.lockClass = lockClass;
            this.count = 1;
        }
    }

    private class LockQueue {
        public List<LockRequest> lockRequests = new ArrayList<LockRequest>();
        public final Lock lock = new ReentrantLock();
    }
    
    private class TransactionControlBlock {
        public final TransactionId tid;
        public List<LockRequest> lockRequests = new ArrayList<LockRequest>();
        public LockRequest wait; // I'm waiting for which lock
        public TransactionControlBlock cycle = null;

        public TransactionControlBlock(TransactionId tid) {
            this.tid = tid;
        }
    }
    
    private ConcurrentHashMap<PageId, LockQueue> lockTable = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TransactionId, TransactionControlBlock> txnTable = new ConcurrentHashMap<>();

    private Thread t;
    
    public LockManager() {
        this.t = new Thread() {
                public void run() {
                    for (;;) {
                        try {
                            Thread.sleep(1000);
                            detectDeadlock();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
        this.t.start();
    }

    public void lock(TransactionId tid, PageId pid, LockMode mode, LockClass lockClass) throws TransactionAbortedException {
        lockPage(tid, pid, mode, lockClass, true);
    }

    public boolean tryLock(TransactionId tid, PageId pid, LockMode mode, LockClass lockClass) {
        try {
            return lockPage(tid, pid, mode, lockClass, false);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    private boolean lockPage(TransactionId tid, PageId pid, LockMode mode, LockClass lockClass, boolean wait) throws TransactionAbortedException {
        LockQueue lockQueue = lockTable.computeIfAbsent(pid, (key) -> new LockQueue());
        lockQueue.lock.lock();
        try {
            LockMode maxGrantedMode = null;
            boolean waiting = false;
            LockRequest lockReq = null;
            ArrayList<LockRequest> convertingRequests = new ArrayList<>();
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
                        convertingRequests.add(lr);
                        break;
                    }
                }
            }
            if (lockReq == null) {
                lockReq = new LockRequest(tid, mode, pid, lockQueue.lock.newCondition(), lockQueue, lockClass);
                if (!waiting && isCompatible(maxGrantedMode, lockReq.mode)) {
                    lockReq.status = LockStatus.GRANTED;
                }
                if (wait) {
                    lockQueue.lockRequests.add(lockReq);
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock(key));
                    synchronized(tcb) {
                        if (lockReq.status != LockStatus.GRANTED) {
                            tcb.wait = lockReq;
                        } else {
                            tcb.wait = null;
                        }
                        tcb.lockRequests.add(lockReq);
                    }
                    while (lockReq.status != LockStatus.GRANTED && lockReq.status != LockStatus.DENIED) {
                        lockReq.notify.awaitUninterruptibly();
                    }
                    if (lockReq.status == LockStatus.DENIED) {
                        lockQueue.lockRequests.remove(lockReq);
                        lockReq.head = null;
                        lockReq.notify = null;
                        synchronized(tcb) {
                            tcb.wait = null;
                            tcb.lockRequests.remove(lockReq);
                        }
                        throw new TransactionAbortedException();
                    }
                    synchronized(tcb) {
                        tcb.wait = null;
                    }
                } else if (lockReq.status == LockStatus.GRANTED) {
                    lockQueue.lockRequests.add(lockReq);                        
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock(key));
                    synchronized(tcb) {
                        tcb.wait = null;
                        tcb.lockRequests.add(lockReq);
                    }
                } else {
                    return false;
                }
            } else {
                // Conversion case
                lockReq.count++;
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
                    } else {
                        // Check if there is a deadlock with another converting request.
                        for (LockRequest lr : convertingRequests) {
                            if (!isCompatible(lockReq.convertMode, lr.mode) && !isCompatible(lr.convertMode, lockReq.mode)) {
                                lockReq.status = LockStatus.DENIED;
                                break;
                            }
                        }
                    }       
                    break;
                }
                if (wait) {
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock(key));
                    synchronized(tcb) {
                        if (lockReq.status == LockStatus.GRANTED) {
                            tcb.wait = null;
                        } else {
                            tcb.wait = lockReq;
                        }
                    }
                    while (lockReq.status != LockStatus.GRANTED && lockReq.status != LockStatus.DENIED) {
                        lockReq.notify.awaitUninterruptibly();
                    }
                    if (lockReq.status == LockStatus.DENIED) {
                        // Clear the conversion info.
                        lockReq.status = LockStatus.GRANTED;
                        lockReq.convertMode = null;
                        lockReq.count--;
                        synchronized(tcb) {
                            tcb.wait = null;
                        }
                        throw new TransactionAbortedException();
                    }
                    // The conversion request is granted.
                    lockReq.convertMode = null;
                    lockReq.lockClass = maxClass(lockReq.lockClass, lockClass);
                } else if (lockReq.status == LockStatus.GRANTED) {
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock(key));
                    synchronized(tcb) {
                        tcb.wait = null;
                    }
                } else {
                    // Clear the conversion info.
                    lockReq.status = LockStatus.GRANTED;
                    lockReq.convertMode = null;
                    lockReq.count--;
                    return false;
                }
            }
        } finally {
            lockQueue.lock.unlock();
        }
        return true;
    }

    public void unlockTransaction(TransactionId tid) {
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
            unlockClass(tid, pid, LockClass.LONG);
        }
        txnTable.remove(tid);
    }

    public void unlock(TransactionId tid, PageId pid) {
        unlockClass(tid, pid, LockClass.SHORT);
    }

    private void unlockClass(TransactionId tid, PageId pid, LockClass lockClass) {
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
                    if (lockClass != LockClass.LONG && (lr.lockClass.ordinal() > lockClass.ordinal() || lr.count > 1)) {
                        lr.count--;
                        return;
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
            lockReq.head = null;
            lockReq.notify = null;
            boolean exists = lockQueue.lockRequests.remove(lockReq);
            assert exists == true;
            TransactionControlBlock tcb = txnTable.get(tid);
            synchronized(tcb) {
                tcb.lockRequests.remove(lockReq);
            }
        } finally {
            lockQueue.lock.unlock();
        }
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

    private void detectDeadlock() {
        for (TransactionControlBlock tcb : txnTable.values()) {
            synchronized(tcb) {
                tcb.cycle = null;
            }
        }
        for (TransactionControlBlock tcb : txnTable.values()) {
            visit(tcb);
        }
    }

    private void visit(TransactionControlBlock me) {
        boolean hasDeadlock = false;
        LockQueue lockQueue;
        synchronized(me) {
            if (me.cycle != null) {
                hasDeadlock = true;
            }
            if (me.wait == null) {
                return;
            }
            lockQueue = me.wait.head;
        }
        if (hasDeadlock) {
            lockQueue.lock.lock();
            try {
                for (LockRequest lr : lockQueue.lockRequests) {
                    if (lr.tid.equals(me.tid)) {
                        lr.status = LockStatus.DENIED;
                        lr.notify.signal();
                        return;
                    }
                }
            } finally {
                lockQueue.lock.unlock();
            }
        }
        
        HashSet<TransactionId> visited = new HashSet<>();
        for (;;) {
            lockQueue.lock.lock();
            TransactionControlBlock cycle = null;
            try {
                synchronized(me) {
                    LockMode waitMode;
                    if (me.wait.status == LockStatus.CONVERTING) {
                        waitMode = me.wait.convertMode;
                    } else if (me.wait.status == LockStatus.WAITING) {
                        waitMode = me.wait.mode;
                    } else {
                        return;
                    }
                    for (LockRequest lr : lockQueue.lockRequests) {
                        if (visited.contains(lr.tid)) {
                            continue;
                        }
                        if (lr.tid.equals(me.tid)) {
                            break;
                        }
                        visited.add(lr.tid);
                        if (!isCompatible(lr.mode, waitMode) || !isCompatible(lr.convertMode, waitMode)) {
                            cycle = me.cycle = txnTable.get(lr.tid);
                            break;
                        }
                    }
                }
            } finally {
                lockQueue.lock.unlock();
            }
            if (cycle == null) {
                return;
            }
            visit(cycle);
            synchronized(me) {
                me.cycle = null;
            }
        }
    }

    private LockClass maxClass(LockClass a, LockClass b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a.ordinal() < b.ordinal()) {
            return b;
        }
        return a;
    }
        
}
