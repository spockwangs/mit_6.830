package simpledb;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LockManagerTest {
    private TransactionId tid1, tid2, tid3;
    private PageId pid1, pid2;
    private LockManager lockManager;
    
    @Before public void setUp() throws Exception {
        this.lockManager = new LockManager();
        this.tid1 = new TransactionId();
        this.tid2 = new TransactionId();
        this.tid3 = new TransactionId();
        this.pid1 = new HeapPageId(0, 0);
        this.pid2 = new HeapPageId(0, 1);
    }

    @Test public void lockOnePage() throws Exception {
        this.lockManager.lockPage(tid1, pid1, LockManager.LockMode.SHARED);
        Assert.assertTrue(this.lockManager.isLocked(tid1, pid1));
        Assert.assertFalse(this.lockManager.isLocked(tid1, pid2));
        Assert.assertFalse(this.lockManager.isLocked(tid2, pid1));
        this.lockManager.unlockPages(tid1);
        Assert.assertFalse(this.lockManager.isLocked(tid1, pid1));
    }

    @Test public void lockWithThreeThreads() throws Exception {
        ArrayBlockingQueue<Integer> toT1 = new ArrayBlockingQueue<>(1);
        ArrayBlockingQueue<Integer> fromT1 = new ArrayBlockingQueue<>(1);
        ArrayBlockingQueue<Integer> toT2 = new ArrayBlockingQueue<>(1);
        ArrayBlockingQueue<Integer> fromT2 = new ArrayBlockingQueue<>(1);
        ArrayBlockingQueue<Integer> toT3 = new ArrayBlockingQueue<>(1);
        ArrayBlockingQueue<Integer> fromT3 = new ArrayBlockingQueue<>(1);

        Thread t1 = new Thread() {
                public void run() {
                    try {
                        toT1.take();
                        lockManager.lockPage(tid1, pid1, LockManager.LockMode.SHARED);
                        fromT1.put(0);

                        toT1.take();
                        lockManager.lockPage(tid1, pid1, LockManager.LockMode.EXCLUSIVE);
                        fromT1.put(0);

                        toT1.take();
                        lockManager.unlockPage(tid1, pid1);
                        fromT1.put(0);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        t1.start();

        Thread t2 = new Thread() {
                public void run() {
                    try {
                        toT2.take();
                        lockManager.lockPage(tid2, pid1, LockManager.LockMode.SHARED);
                        fromT2.put(0);

                        toT2.take();
                        lockManager.unlockPage(tid2, pid1);
                        fromT2.put(0);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        t2.start();

        Thread t3 = new Thread() {
                public void run() {
                    try {
                        toT3.take();
                        lockManager.lockPage(tid3, pid2, LockManager.LockMode.EXCLUSIVE);
                        fromT3.put(0);
                        
                        toT3.take();
                        lockManager.lockPage(tid3, pid1, LockManager.LockMode.EXCLUSIVE);
                        fromT3.put(0);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        t3.start();

        toT1.put(0);
        fromT1.take();
        Assert.assertTrue(lockManager.isLocked(tid1, pid1));

        toT2.put(0);
        fromT2.take();
        Assert.assertTrue(lockManager.isLocked(tid2, pid1));

        toT3.put(0);
        fromT3.take();
        Assert.assertTrue(lockManager.isLocked(tid3, pid2));

        toT3.put(0);
        Assert.assertTrue(fromT3.poll(1000, TimeUnit.MILLISECONDS) == null);
        Assert.assertFalse(lockManager.isLocked(tid3, pid1));

        toT1.put(0);
        Assert.assertTrue(fromT1.poll(1000, TimeUnit.MILLISECONDS) == null);

        toT2.put(0);
        fromT2.take();
        Assert.assertFalse(lockManager.isLocked(tid2, pid1));

        Assert.assertTrue(fromT1.poll(1000, TimeUnit.MILLISECONDS) == 0);
        Assert.assertTrue(lockManager.isLocked(tid1, pid1));
        Assert.assertFalse(lockManager.isLocked(tid3, pid1));

        toT1.put(0);
        Assert.assertTrue(fromT3.poll(1000, TimeUnit.MILLISECONDS) == 0);
        Assert.assertTrue(lockManager.isLocked(tid3, pid1));
        
        t1.join();
        t2.join();
        t3.join();
    }
        
                
    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(LockManagerTest.class);
    }

}

