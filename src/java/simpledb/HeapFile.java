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

    private File file;
    private TupleDesc tupleDesc;
    private int tableId;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {

        file = f;
        tupleDesc = td;
        tableId = file.getAbsoluteFile().hashCode();
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        // some code goes here
        return tableId;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r")){
            int pos = pid.pageNumber() * BufferPool.getPageSize();
            randomAccessFile.seek(pos);
            randomAccessFile.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((double)file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if (!tupleDesc.equals(t.getTupleDesc()))
        {
            throw new DbException("");
        }

        int i;
        HeapPage heapPage;

        for (i = 0; i < numPages(); i ++)
        {
            heapPage = (HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_WRITE));
            if (heapPage.getNumEmptySlots() > 0)
            {
                break;
            }

        }
        if (i == numPages())
        {
            heapPage = new HeapPage(new HeapPageId(this.getId(), i), HeapPage.createEmptyPageData());

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(BufferPool.getPageSize() * heapPage.getId().pageNumber());
            randomAccessFile.write(heapPage.getPageData());
            randomAccessFile.close();
        }

        heapPage = (HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_WRITE));
        heapPage.insertTuple(t);

        ArrayList<Page> pList = new ArrayList<Page>();
        pList.add(heapPage);
        return pList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        if (tableId != t.getRecordId().getPageId().getTableId())
        {
            throw new DbException("");
        }

        HeapPage heapPage = (HeapPage)(Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE));
        heapPage.deleteTuple(t);

        ArrayList<Page> pageArrayList= new ArrayList<>();
        pageArrayList.add(heapPage);
        return pageArrayList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private int pageNum;
        private Iterator<Tuple> tuplesInPage;

        public HeapFileIterator(TransactionId tid)
        {
            this.tid = tid;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNum = 0;
            HeapPageId pid = new HeapPageId(getId(), pageNum);
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuplesInPage == null)
            {
                return false;
            }
            if (tuplesInPage.hasNext())
            {
                return true;
            }
            if (pageNum < numPages() - 1)
            {
                pageNum++;
                HeapPageId pid = new HeapPageId(getId(), pageNum);
                tuplesInPage = getTuplesInPage(pid);
                return tuplesInPage.hasNext();
            }
            else
            {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            pageNum = 0;
            tuplesInPage = null;
        }
    }
}

