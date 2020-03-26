package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //my code
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer>GroupByMap;
    private Map<Field, List<Integer>>AvgMap;

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
        this.what = what;
        this.GroupByMap = new HashMap<>();
        this.AvgMap = new HashMap<>();
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
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        if(gbfield != null&&this.gbfieldtype != gbfield.getType()){
            throw new IllegalArgumentException();
        }
        IntField afield = (IntField)tup.getField(this.afield);
        int value = afield.getValue();
        switch (this.what){
            case MIN:
                if(this.GroupByMap.containsKey(gbfield)){
                    this.GroupByMap.put(gbfield, Math.min(this.GroupByMap.get(gbfield), value));
                }
                else this.GroupByMap.put(gbfield, value);
                break;
            case MAX:
                if(this.GroupByMap.containsKey(gbfield)){
                    this.GroupByMap.put(gbfield, Math.max(this.GroupByMap.get(gbfield), value));
                }
                else this.GroupByMap.put(gbfield, value);
                break;
            case COUNT:
                if(this.GroupByMap.containsKey(gbfield)){
                    this.GroupByMap.put(gbfield, this.GroupByMap.get(gbfield)+1);
                }
                else this.GroupByMap.put(gbfield, 1);
                break;
            case SUM:
                if(this.GroupByMap.containsKey(gbfield)){
                    this.GroupByMap.put(gbfield, this.GroupByMap.get(gbfield)+value);
                }
                else this.GroupByMap.put(gbfield, value);
                break;
            case AVG:
                if(this.AvgMap.containsKey(gbfield)){
                    List<Integer>list = this.AvgMap.get(gbfield);
                    list.add(value);
                }
                else{
                    List<Integer>list = new ArrayList<>();
                    list.add(value);
                    this.AvgMap.put(gbfield, list);
                }
                break;
            default:
                throw new IllegalArgumentException();
        }
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
        List<Tuple>tupleList = new ArrayList<>();
        if(this.gbfield != NO_GROUPING){
            TupleDesc tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal","aggregateVal"});
            if(this.what == Op.AVG){
                Iterator<Map.Entry<Field, List<Integer>>> it = this.AvgMap.entrySet().iterator();
                for (int i = 0; i < this.AvgMap.entrySet().size(); i++) {
                    Map.Entry<Field, List<Integer>>entry = it.next();
                    int sum = 0;
                    for (int j = 0; j < entry.getValue().size(); j++) {
                        sum += entry.getValue().get(j);
                    }
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(sum / entry.getValue().size()));
                    tupleList.add(tuple);
                }
            }
            else{
                Iterator<Map.Entry<Field, Integer>> it = this.GroupByMap.entrySet().iterator();
                for (int i = 0; i < this.GroupByMap.entrySet().size(); i++) {
                    Map.Entry<Field, Integer>entry = it.next();
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                    tupleList.add(tuple);
                }
            }
            return new TupleIterator(tupleDesc, tupleList);
        }
        else{
            TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            if(this.what == Op.AVG) {
                Iterator<List<Integer>> it = this.AvgMap.values().iterator();
                for (int i = 0; i < this.AvgMap.values().size(); i++) {
                    List<Integer> list = it.next();
                    int sum = 0;
                    for (int j = 0; j < list.size(); j++) {
                        sum += list.get(j);
                    }
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(sum / list.size()));
                    tupleList.add(tuple);
                }
            }
            else {
                Iterator<Integer>it = this.GroupByMap.values().iterator();
                for (int i = 0; i < this.GroupByMap.values().size(); i++) {
                    int aggregateVal = it.next();
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(aggregateVal));
                    tupleList.add(tuple);
                }
            }
            return new TupleIterator(tupleDesc, tupleList);
        }
    }

}
