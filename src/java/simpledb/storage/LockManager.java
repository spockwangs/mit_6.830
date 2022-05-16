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

    public enum LockStatus {
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
        System.err.println("LockManager\n");
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

    public void lockPage(TransactionId tid, PageId pid, LockMode mode) throws TransactionAbortedException {
        System.out.println("lockPage: tid=" + tid.getId() + ", pid=" + pid.getPageNumber());
        lockPage(tid, pid, mode, true);
        System.out.println("lockedPage: tid=" + tid.getId() + ", pid=" + pid.getPageNumber());
    }

    public boolean tryLockPage(TransactionId tid, PageId pid, LockMode mode) {
        try {
            return lockPage(tid, pid, mode, false);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    private boolean lockPage(TransactionId tid, PageId pid, LockMode mode, boolean wait) throws TransactionAbortedException {
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
                lockReq.head = lockQueue;
                lockReq.notify = lockQueue.lock.newCondition();
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
                        lockQueue.lockRequests.remove(lockReq);
                        lockReq.head = null;
                        lockReq.notify = null;
                        synchronized(tcb) {
                            tcb.wait = null;
                            tcb.lockRequests.remove(lockReq);
                        }
                        throw new TransactionAbortedException();
                    }                        
                } else if (lockReq.status == LockStatus.GRANTED) {
                    TransactionControlBlock tcb = txnTable.computeIfAbsent(tid, (key) -> new TransactionControlBlock(key));
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
            lockReq.head = null;
            lockReq.notify = null;
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

    private void detectDeadlock() {
        for (TransactionControlBlock tcb : txnTable.values()) {
            synchronized(tcb) {
                tcb.cycle = null;
            }
        }
        for (TransactionControlBlock tcb : txnTable.values()) {
            System.out.println("begin visit");
            visit(tcb);
            System.out.println("end visit");
        }
    }

    private void visit(TransactionControlBlock me) {
        System.out.println("visiting " + me.tid.getId());

        boolean hasDeadlock = false;
        LockQueue lockQueue;
        synchronized(me) {
            if (me.cycle != null) {
                hasDeadlock = true;
            }
            System.out.println("hasDeadlock=" + hasDeadlock);
            if (me.wait == null) {
                System.out.println("exit visit");
                return;
            }
            lockQueue = me.wait.head;
        }
        if (hasDeadlock) {
            lockQueue.lock.lock();
            try {
                System.out.println("deadlock deny " + me.tid.getId());
                for (LockRequest lr : lockQueue.lockRequests) {
                    if (lr.tid.equals(me.tid)) {
                        lr.status = LockStatus.DENIED;
                        lr.notify.signal();
                        System.out.println("exit visit");
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
                        System.out.println("exit visit");
                        return;
                    }
                    System.out.println("waitMode=" + waitMode);
                    for (LockRequest lr : lockQueue.lockRequests) {
                        System.out.println("lr.tid=" + lr.tid.getId() + ", lr.pid=" + lr.pid.getPageNumber());
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
                System.out.println("exit visit cycle null");
                return;
            }
            visit(cycle);
            synchronized(me) {
                me.cycle = null;
            }
        }
    }
        
}
