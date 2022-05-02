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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;

    private class AggregateValue {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum = 0;
        private int count = 0;
        private Op op;
        
        public AggregateValue(Op op) {
            this.op = op;
        }
        
        public void consume(int val) {
            switch (this.op) {
            case MIN:
                this.min = Math.min(val, this.min);
                break;
            case MAX:
                this.max = Math.max(val, this.max);
                break;
            case SUM:
                this.sum += val;
                break;
            case COUNT:
                this.count += 1;
                break;
            case AVG:
                this.sum += val;
                this.count += 1;
                break;
            }
        }

        public int getResult() {
            switch (this.op) {
            case MIN:
                return this.min;
            case MAX:
                return this.max;
            case SUM:
                return this.sum;
            case COUNT:
                return this.count;
            case AVG:
                return this.sum / this.count;
            }
            return 0;
        }
    };
    
    // Map group by field => AggregateValue
    private HashMap<Field, AggregateValue> m;
    private AggregateValue av;
    
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if (this.gbfield == NO_GROUPING) {
            this.av = new AggregateValue(this.op);
        } else {
            this.m = new HashMap<Field, AggregateValue>();
        }
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
        AggregateValue val = this.av;
        if (this.gbfield != NO_GROUPING) {
            Field groupByField = tup.getField(this.gbfield);
            val = this.m.get(groupByField);
            if (val == null) {
                val = new AggregateValue(this.op);
                this.m.put(groupByField, val);
            }
        }

        IntField inputField = (IntField) tup.getField(this.afield);
        int inputVal = inputField.getValue();
        val.consume(inputVal);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td;
        if (this.gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(this.av.getResult()));
            tuples.add(t);
        } else {
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
            for (Map.Entry<Field, AggregateValue> e : this.m.entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, e.getKey());
                t.setField(1, new IntField(e.getValue().getResult()));
                tuples.add(t);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
