package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //my code
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> GroupByMap;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.GroupByMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        if(gbfield != null&&this.gbfieldtype != gbfield.getType()){
            throw new IllegalArgumentException();
        }
        if (this.GroupByMap.containsKey(gbfield)) {
            this.GroupByMap.put(gbfield, this.GroupByMap.get(gbfield) + 1);
        }
        else this.GroupByMap.put(gbfield, 1);
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
        List<Tuple>tupleList = new ArrayList<>();
        if(this.gbfield != NO_GROUPING){
            TupleDesc tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal","aggregateVal"});
            Iterator<Map.Entry<Field, Integer>> it = this.GroupByMap.entrySet().iterator();
            for (int i = 0; i < this.GroupByMap.entrySet().size(); i++) {
                Map.Entry<Field, Integer>entry = it.next();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tupleList.add(tuple);
            }
            return new TupleIterator(tupleDesc, tupleList);
        }
        else{
            TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            Iterator<Integer>it = this.GroupByMap.values().iterator();
            for (int i = 0; i < this.GroupByMap.values().size(); i++) {
                int aggregateVal = it.next();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(aggregateVal));
                tupleList.add(tuple);
            }
            return new TupleIterator(tupleDesc, tupleList);
        }
    }

}
