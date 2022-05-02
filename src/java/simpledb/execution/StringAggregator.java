package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private HashMap<Field, Integer> m;
    private int count;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (this.gbfield == NO_GROUPING) {
            this.count = 0;
        } else {
            this.m = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbfield == NO_GROUPING) {
            ++this.count;
        } else {
            Field key = tup.getField(this.gbfield);
            Integer count = this.m.get(key);
            if (count == null) {
                this.m.put(key, 1);
            } else {
                this.m.put(key, count+1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td;
        if (this.gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(this.count));
            tuples.add(t);
        } else {
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
            for (Map.Entry<Field, Integer> e : this.m.entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, e.getKey());
                t.setField(1, new IntField(e.getValue().intValue()));
                tuples.add(t);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
