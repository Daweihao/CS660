package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        public int getSize(){
            int size = fieldType.getLen();
            return  size;
        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdList.iterator();
    }

    public ArrayList<TDItem> tdList;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tdList = new ArrayList<>();
        int length = typeAr.length;
        for (int i = 0; i < length ; i++) {
            TDItem tdItem = new TDItem(typeAr[i],fieldAr[i]);
            this.tdList.add(tdItem);
        }

        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tdList = new ArrayList<>();
        int length = typeAr.length;
        for (int i = 0; i < length ; i++) {
            TDItem tdItem = new TDItem(typeAr[i],null);
            this.tdList.add(tdItem);
        }
    }

    public TupleDesc(ArrayList<TDItem> tdList){
        this.tdList = tdList;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {

        int num = this.tdList.size();
        // some code goes here
        return num;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > tdList.size() - 1 )
            throw new NoSuchElementException("Invalid field reference!");
        String fieldName = this.tdList.get(i).fieldName;
        return fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i > tdList.size() - 1 )
            throw new NoSuchElementException("Invalid field reference!");
        Type type = this.tdList.get(i).fieldType;
        // some code goes here
        return type;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null)
            throw new NoSuchElementException();
        for (TDItem tdItem : this.tdList)
        {
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name))
                return this.tdList.indexOf(tdItem);
            else if (tdItem.fieldName == null)
                throw new NoSuchElementException();
        }
        throw new NoSuchElementException(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdItem : this.tdList){
            size = size + tdItem.getSize();
        }
        // some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
//        Type[] typeAr  = new Type[td1.numFields() + td2.numFields()];
//        int i = 0;
//        for (; i < typeAr.length; i++) {
//            if (i < td1.numFields())
//            typeAr[i] = td1.getFieldType(i);
//            else
//                typeAr[i] = td2.getFieldType(i);
//        }
        ArrayList<TDItem> tdList1 = new ArrayList<>();
        tdList1.addAll(td1.tdList);
        tdList1.addAll(td2.tdList);
        TupleDesc td0 = new TupleDesc(tdList1);

        // some code goes here
        return td0;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o.getClass() == this.getClass()) {
            if (this.getSize() == ((TupleDesc) o).getSize()) {
                for (int i = 0; i < this.numFields(); i++) {
                    if (this.getFieldType(i) == ((TupleDesc) o).getFieldType(i))
                        return true;
                }
            }
        }
        // some code goes here
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String organizer = "";
        for (Iterator iter = this.iterator();iter.hasNext();){
            String str = iter.next().toString();
            organizer += (str + ", ");
        }
        return organizer;
    }
}
