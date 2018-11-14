package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid ;
    private volatile int lastEmptyPage = -1;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId id = (HeapPageId) pid;
        BufferedInputStream bis = null;

        try {
            bis = new BufferedInputStream(new FileInputStream(f));
            byte pageBuf[] = new byte[BufferPool.getPageSize()];
            if (bis.skip(id.pageNumber() * BufferPool.getPageSize()) != id
                    .pageNumber() * BufferPool.getPageSize()) {
                throw new IllegalArgumentException(
                        "Unable to seek to correct place in heapfile");
            }
            int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from heapfile");
            }
            Debug.log(1, "HeapFile.readPage: read page %d", id.pageNumber());
            HeapPage p = new HeapPage(id, pageBuf);
            return p;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            // Close the file on success or error
            try {
                if (bis != null)
                    bis.close();
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        HeapPage p = (HeapPage) page;
		
        byte[] data = p.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(p.getId().pageNumber() * BufferPool.getPageSize());
        rf.write(data);
        rf.close();
        // not necessary for lab1|lab2
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtypages = new ArrayList<Page>();
        int i = 0;
        if (lastEmptyPage != -1)
            i = lastEmptyPage;
        for (; i < numPages(); i++) {
            Debug.log(
                    4,
                    "HeapFile.addTuple: checking free slots on page %d of table %d",
                    i, tableid);
            HeapPageId pid = new HeapPageId(tableid, i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                    Permissions.READ_WRITE);
            if (p.getNumEmptySlots() == 0) {
                Debug.log(
                        4,
                        "HeapFile.addTuple: no free slots on page %d of table %d",
                        i, tableid);
                if (lastEmptyPage != -1) {
                    lastEmptyPage = -1;
                    break;
                }
                continue;
            }
            Debug.log(4, "HeapFile.addTuple: %d free slots in table %d",
                    p.getNumEmptySlots(), tableid);
            p.insertTuple(t);
            lastEmptyPage = p.getId().pageNumber();
            dirtypages.add(p);
            return dirtypages;
        }
        // some code goes here
        synchronized (this) {
            BufferedOutputStream bw = new BufferedOutputStream(
                    new FileOutputStream(f, true));
            byte[] emptyData = HeapPage.createEmptyPageData();
            bw.write(emptyData);
            bw.close();
        }
        HeapPage p = (HeapPage) Database.getBufferPool()
                .getPage(tid, new HeapPageId(tableid, numPages() - 1),
                        Permissions.READ_WRITE);
        p.insertTuple(t);
        lastEmptyPage = p.getId().pageNumber();
        dirtypages.add(p);
        return dirtypages;

        // not necessary for lab1|lab2
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(
                tid,
                new HeapPageId(tableid, t.getRecordId().getPageId()
                        .pageNumber()), Permissions.READ_WRITE);
        p.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<Page>();
        pages.add(p);
        return pages;
        // not necessary for lab1|lab2
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    TransactionId tid;
    HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() throws DbException, TransactionAbortedException {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
