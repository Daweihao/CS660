package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{
    private TransactionId tid;
    private HeapFile hf;
    private Iterator<Tuple> i;
    private int pgNum;

    public HeapFileIterator(TransactionId tid, HeapFile hf) {
        this.tid = tid;
        this.hf = hf;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pgNum = 0;
        i = getTupleLsFrPg(pgNum).iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if( i == null){
            return false;
        }
        if(i.hasNext()){
            return true;
        } else if (pgNum < hf.numPages()-1){
            // if we have more pages to iterate
            if(getTupleLsFrPg(pgNum + 1).size() > 0){
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        //Skip null tuple
        if(i == null){
            throw new NoSuchElementException("tuple is null");
        }

        if(i.hasNext()){
            //there are tuples available on page
            Tuple t = i.next();
            return t;
        } else if(!i.hasNext() && pgNum < hf.numPages()-1) {
            //tuples are on next page
            pgNum ++;
            i = getTupleLsFrPg(pgNum).iterator();
            if (i.hasNext())
                return i.next();
            else {
                throw new NoSuchElementException("No more Tuples");
            }

        } else {
            // no more tuples on current page and no more pages in file
            throw new NoSuchElementException("No more Tuples");
        }

    }
    // Returns a list of tuples from page
    private List<Tuple> getTupleLsFrPg(int pgNum) throws TransactionAbortedException, DbException{

        PageId pageId = new HeapPageId(hf.getId(), pgNum);
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);

        List<Tuple> tupleList = new ArrayList<Tuple>();

        // get all tuples from the first page in the file
        HeapPage hp = (HeapPage)page;
        Iterator<Tuple> itr = hp.iterator();
        while(itr.hasNext()){
            tupleList.add(itr.next());
        }
        return  tupleList;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();

    }

    @Override
    public void close() {
        i = null;

    }
}
