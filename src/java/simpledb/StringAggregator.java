package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<String, ArrayList<String>> stringHashmap;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        stringHashmap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key = "NoGrouping";
        if(gbfield != Aggregator.NO_GROUPING && gbfieldtype == Type.INT_TYPE){
            key = Integer.toString(((IntField)tup.getField(gbfield)).getValue());
        } else if(gbfield != Aggregator.NO_GROUPING){
            key = ((StringField)tup.getField(gbfield)).getValue();
        }
        String temp = ((StringField)tup.getField(afield)).getValue();
        if(!stringHashmap.containsKey(key))
            stringHashmap.put(key, new ArrayList<>());
        stringHashmap.get(key).add(temp);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator(){
            private Iterator iter = null;
            private int calculation(ArrayList<String> list) throws DbException{
                int result = 0;
                if(what == what.COUNT)
                    return list.size();
                throw new DbException("");
            }
            @Override
            public void open() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                iter = stringHashmap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                if(iter == null)
                    throw new DbException("");
                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // TODO Auto-generated method stub
                if(iter == null)
                    throw new DbException("");
                if(!iter.hasNext())
                    throw new NoSuchElementException("");

                TupleDesc td = getTupleDesc();
                Tuple tuple = new Tuple(td);
                HashMap.Entry temp = (HashMap.Entry) iter.next();
                ArrayList<String> list = (ArrayList<String>) temp.getValue();
                String key = (String) temp.getKey();
                IntField value = new IntField(calculation(list));
                if(gbfield == Aggregator.NO_GROUPING){
                    tuple.setField(0, value);
                }else if(gbfieldtype == Type.INT_TYPE){
                    IntField gbfieldvalue = new IntField(Integer.parseInt(key));
                    tuple.setField(0, gbfieldvalue);
                    tuple.setField(1, value);
                }else { // stringfield
                    StringField gbfieldvalueString = new StringField(key, key.length());
                    tuple.setField(0, gbfieldvalueString);
                    tuple.setField(1, value);
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                // TODO Auto-generated method stub
                if(gbfield == Aggregator.NO_GROUPING)
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub
                iter = null;
            }

        };
    }

}
