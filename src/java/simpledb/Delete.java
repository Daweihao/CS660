package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private Tuple reTuple;
    private boolean valid;
    private int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.child = child;
        this.t = t;
        this.count = 0;
        Type[] typeAr = {Type.INT_TYPE};
        String[] fieldAr = {"Deleted_counts"};
        this.reTuple = new Tuple(new TupleDesc(typeAr,fieldAr));
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return reTuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        count = 0;
        valid = true;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        valid = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        valid = true;
        count = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        if (!valid || child == null){
            return null;
        }
        while (child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(t,child.next());
                count ++;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchElementException e){
                e.printStackTrace();
            }

        }
            reTuple.setField(0,new IntField(count));
            valid = false;
            return reTuple;


    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
