package simpledb;

import java.io.Serializable;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see simpledb.IndexDbIterator
 */
public class IndexPredicate implements Serializable {
	
    private static final long serialVersionUID = 1L;
	
	// <silentstrip lab1|lab2>
    private Predicate.Op op;
    private Field fieldvalue;
    // </silentstrip>

    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public IndexPredicate(Predicate.Op op, Field fvalue) {
        // <strip lab1|lab2>
        this.op = op;
        this.fieldvalue = fvalue;
        // </strip>
        // <insert lab1>
        // // not necessary for lab1
        // </insert>
    }

    public Field getField() {
        // <strip lab1|lab2>
        return fieldvalue;
        // </strip>
        // <insert lab1>
        // // not necessary for lab1
        // </insert>
        // <insert lab1|lab2>
        // return null;
        // </insert>
    }

    public Predicate.Op getOp() {
        // <strip lab1|lab2>
        return op;
        // </strip>
        // <insert lab1>
        // // not necessary for lab1
        // </insert>
        // <insert lab1|lab2>
        // return null;
        // </insert>
    }

    /** Return true if the fieldvalue in the supplied predicate
        is satisfied by this predicate's fieldvalue and
        operator.
        @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        // <strip lab1|lab2>
        if (ipd == null)
            return false;
        return (op.equals(ipd.op) && fieldvalue.equals(ipd.fieldvalue));
        // </strip>
        // <insert lab1>
        // // not necessary for lab1
        // </insert>
        // <insert lab1|lab2>
        // return false;
        // </insert>
    }

}
