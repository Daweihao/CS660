package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<String, ArrayList<Integer>> integerHashmap;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        integerHashmap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key = "NoGrouping";
        if(gbfield != Aggregator.NO_GROUPING && gbfieldtype == Type.INT_TYPE){
            key = Integer.toString(((IntField)tup.getField(gbfield)).getValue());
        } else if(gbfield != Aggregator.NO_GROUPING){
            key = ((StringField)tup.getField(gbfield)).getValue();
        }
        Integer temp = ((IntField)tup.getField(afield)).getValue();
        if(!integerHashmap.containsKey(key))
            integerHashmap.put(key, new ArrayList<>());
        integerHashmap.get(key).add(temp);
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
        return new DbIterator(){
            private Iterator iter = null;
            public int calculation(ArrayList<Integer> list) throws NoSuchElementException{
                int result = 0;
                switch(what){
                    case MAX:
                        return Collections.max(list);
                    case MIN:
                        return Collections.min(list);
                    case AVG:
                        for(int i : list)
                            result = result + i;
                        return result / list.size();
                    case SUM:
                        for(int i : list)
                            result = result + i;
                        return result;
                    case COUNT:
                        return list.size();
                    default:
                        throw new NoSuchElementException("");
                }
            }
            @Override
            public void open() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                iter = integerHashmap.entrySet().iterator();
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
                ArrayList<Integer> list = (ArrayList<Integer>) temp.getValue();
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
