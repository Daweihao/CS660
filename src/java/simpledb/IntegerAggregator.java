package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field,Group> groups;
    private Group noGroup;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.what = what;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.groups = new HashMap<>();
        this.noGroup = new Group(null);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING || gbfieldtype == null){
            noGroup.tuples.add(tup);
        } else {
            if (groups.containsKey(tup.getField(gbfield))){
                groups.get(tup.getField(gbfield)).tuples.add(tup);
            } else {
                Group newGroup = new Group(tup.getField(gbfield));
                groups.put(tup.getField(gbfield),newGroup);
            }
        }
        // some code goes here
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
            Iterator<Group> groupIt;
            TupleDesc td;
            @Override
            public void open() throws DbException, TransactionAbortedException {

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

            }

            @Override
            public TupleDesc getTupleDesc() {
                return null;
            }

            @Override
            public void close() {

            }
        };

    }

}
