package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TransactionId tid;
    private TupleDesc countTup;
    private boolean isItInYet;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	Type[] type = new Type[1];
       	type[0] = Type.INT_TYPE;
    	String[] name = new String[1];
    	name[0] = "DeleteCount";
    	this.countTup = new TupleDesc(type, name);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.countTup;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        child.open();
        this.isItInYet = false;
    }

    public void close() {
    	super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.isItInYet){
    		return null;
    	}
    	this.isItInYet = true;
    	int count = 0;
    	BufferPool goFetch = Database.getBufferPool();
    	Tuple nextTup;
    	while(child.hasNext()){
    		nextTup = child.next();
    		goFetch.deleteTuple(tid, nextTup);
    		
    		count++;
    	}
    	Field f = new IntField(count);
    	Tuple deleteCount = new Tuple(countTup);
    	deleteCount.setField(0, f);
        return deleteCount;
    }
    
    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child != children[0])
        {
            this.child = children[0];
        }
    }

}
