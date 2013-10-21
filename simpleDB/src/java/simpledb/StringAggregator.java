package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private String aName;
	private String groupName;
	private static HashMap<Field,Field> aggr;
    private static final long serialVersionUID = 1L;

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
    	aggr = new HashMap<Field,Field>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	aName = tup.getTupleDesc().getFieldName(afield);
    	Field aggrKey;
    	if(gbfield == NO_GROUPING){
    		aggrKey = new IntField(NO_GROUPING);
    	}
    	else{
    		aggrKey = tup.getField(gbfield);
    		groupName = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	IntField aggrValue = (IntField)aggr.get(aggrKey);
    	int count;
    	if(aggrValue != null){
    		count = aggrValue.getValue()+1;
    	}
    	else{
    		count = 1;
    	}
    	Field countValue = new IntField(count);
    	aggr.put(aggrKey, countValue);
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
        ArrayList<Tuple> _tuples = new ArrayList<Tuple>();
        Type[] fieldType;
        String[] fieldName;
        if(gbfield == NO_GROUPING){
        	fieldType = new Type[1];
    		fieldName = new String[1];
    		fieldType[0] = Type.INT_TYPE;
    		fieldName[0]= aName;
    		TupleDesc tupdesc = new TupleDesc(fieldType,fieldName);
    		Tuple groupTuple = new Tuple(tupdesc);
    		groupTuple.setField(0, aggr.get(new IntField(NO_GROUPING)));
    		_tuples.add(groupTuple);
    		return new TupleIterator(tupdesc,_tuples);
        }
        else{
        	fieldType = new Type[2];
    		fieldName = new String[2];
    		fieldType[0] = gbfieldtype;
    		fieldName[0] = groupName;
    		fieldType[1] = Type.INT_TYPE;
    		fieldName[1] = aName;
    		TupleDesc tupdesc = new TupleDesc(fieldType,fieldName);
    		Iterator<Field> keys = aggr.keySet().iterator();
    		while(keys.hasNext()){
    			Field nxt = keys.next();
    			Tuple groupTuple = new Tuple(tupdesc);
    			groupTuple.setField(0,nxt);
    			groupTuple.setField(1,aggr.get(nxt));
    			_tuples.add(groupTuple);
    			
    		}
    		return new TupleIterator(tupdesc,_tuples);
        }
    }

}
