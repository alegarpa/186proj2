package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int tableid;
    private TransactionId tid;
    private TupleDesc countTup;
    private boolean isItInYet;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        	this.tid = t;
        	this.child = child;
        	this.tableid = tableid;
        	Type[] type = new Type[1];
        	type[0] = Type.INT_TYPE;
        	String[] name = new String[1];
        	name[0] = "InsertCount";
        	this.countTup = new TupleDesc(type, name);
        	
        	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.countTup;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    	this.isItInYet = false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.isItInYet){
    		return null;
    	}
    	int count = 0;
    	BufferPool goFetch = Database.getBufferPool();
    	Tuple nextTup;
    	//I DIDN'T CHECK FOR MOTHER F DUPLICATES OH MAH GAWD THIS IS BS
    	this.isItInYet = true;
    	while(child.hasNext()){
    		
    		nextTup = child.next();
    		try{
    		goFetch.insertTuple(tid, tableid, nextTup);
    		}
    		catch (IOException e) {
    			e.printStackTrace();
    		}
    		count++;
    	}
    	Field f = new IntField(count);
    	Tuple insertCount = new Tuple(countTup);
    	insertCount.setField(0, f);
        return insertCount;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
       this.child = children[0];
    	
    }
}
