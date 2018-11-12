package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private Tuple retTup;
    private boolean valid;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        Type[] typeAr = {Type.INT_TYPE};
        String[] fieldAr = {"Inserted_counts"};
        this.retTup = new Tuple(new TupleDesc(typeAr,fieldAr));
        this.valid = true;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return retTup.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        valid = true;
        // some code goes here
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int count = 0;

        while (child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(t,tableId,child.next());
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (valid) {
            retTup.setField(0, new IntField(count));
            valid = false;
            return retTup;
        }else {
            return null;
        }
        // some code goes here

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
