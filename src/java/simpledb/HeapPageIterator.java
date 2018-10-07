package simpledb;

import java.util.ArrayList;
import java.util.Iterator;

public class HeapPageIterator implements Iterator<Tuple> {
    ArrayList<Tuple> tuples;
    Iterator<Tuple> i ;
    public HeapPageIterator(ArrayList<Tuple> tuples){
        this.tuples = tuples;
        this.i = tuples.iterator();
    }
    @Override
    public boolean hasNext() {
        return i.hasNext();
    }

    @Override
    public simpledb.Tuple next() {
        return i.next();
    }

    @Override
    public void remove() {
    throw new UnsupportedOperationException("remove");
    }
}
