package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private static HashMap<Field,Field> aggr;
	private HashMap<Field,Integer> countMap = new HashMap<Field,Integer>();
	private HashMap<Field,Integer> sumMap = new HashMap<Field,Integer>();
	private String groupName;
	private String aName;
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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	aggr = new HashMap<Field,Field>();
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
    	Field aggrKey;
    	if(gbfield == NO_GROUPING){
    		aName = tup.getTupleDesc().getFieldName(afield);
    		aggrKey = new IntField(NO_GROUPING);
    	}
    	else{
    		aggrKey = tup.getField(gbfield);
    		aName = tup.getTupleDesc().getFieldName(afield);
    		groupName = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	
    
    	IntField aggrValue = (IntField)aggr.get(aggrKey);
    	IntField aggrField = (IntField)tup.getField(afield);
    	
    	if(what.equals(Op.COUNT)){
    		int count;
    		if(aggrValue != null){
    			count = aggrValue.getValue()+1;
    		}
    		else{
    			count = 1;
    		}
    		Field countValue = new IntField(count);
    		aggr.put(aggrKey,countValue);
    	}
    	if(what.equals(Op.SUM)){
    		int sum;
    		if(aggrValue != null){
    			sum = aggrValue.getValue();
    		}
    		else{
    			sum = 0;
    		}
    		sum += aggrField.getValue();
    		Field current_sumValue = new IntField(sum);
    		aggr.put(aggrKey,current_sumValue);
    		
    	}
    	if(what.equals(Op.AVG)){
    		int sum;
    		int count;
    		int avg;
    		if(aggrValue != null){
    			count = countMap.get(aggrKey) +1 ;
    			sum = sumMap.get(aggrKey) + aggrField.getValue();
    			sumMap.put(aggrKey,sum);
    			countMap.put(aggrKey, count);
    			avg = sum/count;
    			
    		}
    		else{
    			sum = aggrField.getValue();
    			avg = aggrField.getValue();
    			count = 1;
    			sumMap.put(aggrKey,sum);
    			countMap.put(aggrKey,count);
    			
    			
    		}
    		Field avgValue = new IntField(avg);
    		aggr.put(aggrKey, avgValue);
    		
    	}
    	if(what.equals(Op.MAX)){
    		int max;
    		if(aggrValue != null){
    			max = Math.max((aggrField.getValue()), aggrValue.getValue());
    		}
    		else{
    			max = aggrField.getValue();
    			
    		}
    		Field newMax = new IntField(max);
    		aggr.put(aggrKey,newMax);
    	}
    	if(what.equals(Op.MIN)){
    		int min;
    		if(aggrValue != null){
    			min = Math.min((aggrField.getValue()), aggrValue.getValue());
    		}
    		else{
    			min = aggrField.getValue();
    			
    		}
    		Field newMin = new IntField(min);
    		aggr.put(aggrKey,newMin);
    	}
    		
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
    	ArrayList<Tuple> _tuples = new ArrayList<Tuple>();
    	Type[] fieldType;
    	String[] fieldName;
    	if (gbfield == NO_GROUPING){
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
