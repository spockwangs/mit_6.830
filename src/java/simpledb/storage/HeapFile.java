package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final ReentrantLock appendLock = new ReentrantLock();
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            if (pid.getPageNumber() >= this.numPages()) {
                throw new IllegalArgumentException();
            }
        
            int pageSize = Database.getBufferPool().getPageSize();
            int start = pid.getPageNumber() * pageSize;
            byte[] data = new byte[pageSize];
            FileInputStream fin = new FileInputStream(this.file);
            fin.skip(start);
            fin.read(data);
            HeapPageId hPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            HeapPage page = new HeapPage(hPageId, data);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
            return null;      // should not happen
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        RandomAccessFile writer = new RandomAccessFile(this.file, "rw");
        long offset = Database.getBufferPool().getPageSize() * pgNo;
        writer.seek(offset);
        writer.write(page.getPageData());
        writer.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long pageSize = Database.getBufferPool().getPageSize();
        return (int) (this.file.length() / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> result = new ArrayList<Page>();
        BufferPool bp = Database.getBufferPool();
        for (;;) {
            int numOfPages = this.numPages();
            for (int i = 0; i < numOfPages; ++i) {
                PageId pid = new HeapPageId(this.getId(), i);
                HeapPage page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_ONLY);
                if (page.getNumEmptySlots() > 0) {
                    bp.getPage(tid, pid, Permissions.READ_WRITE);
                    page.insertTuple(t);
                    result.add(page);
                    return result;
                } else {
                    bp.unsafeReleasePage(tid, pid);
                }
            }

            // Append an empty page.
            this.appendLock.lock();
            try {
                if (numOfPages == this.numPages()) {
                    BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(this.file, true));
                    byte[] emptyData = HeapPage.createEmptyPageData();
                    bw.write(emptyData);
                    bw.close();
                }
            } finally {
                this.appendLock.unlock();
            }
            PageId pid = new HeapPageId(this.getId(), this.numPages()-1);
            HeapPage page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                result.add(page);
                return result;
            }
            bp.unsafeReleasePage(tid, pid);
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId() == null) {
            throw new DbException("no rid");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
                                                                    t.getRecordId().getPageId(),
                                                                    Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> result = new ArrayList<Page>();
        result.add(page);
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int curPageNo = 0;
            private Iterator<Tuple> it = null;
            
            private Iterator<Tuple> getPageIterator(int pageNo) throws DbException, TransactionAbortedException {
                HeapPageId pageId = new HeapPageId(getId(), curPageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                return page.iterator();
            }
            
            @Override 
            public void open() throws DbException, TransactionAbortedException {
                it = getPageIterator(curPageNo);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (it == null) {
                    return false;
                }
                while (!it.hasNext()) {
                    ++curPageNo;
                    if (curPageNo >= numPages()) {
                        return false;
                    }
                    it = getPageIterator(curPageNo);
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()) {
                    return it.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curPageNo = 0;
                it = getPageIterator(curPageNo);
            }

            @Override
            public void close() {
                curPageNo = 0;
                it = null;
            }
        };
    }

}

