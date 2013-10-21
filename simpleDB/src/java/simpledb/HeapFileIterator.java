package simpledb;

import java.util.NoSuchElementException;
import java.util.*;
public class HeapFileIterator implements DbFileIterator {
	private int current_page;
	private int number_pages;
	private Iterator<Tuple> Tuple_it;
	private HeapPage curr_HPage;
	private HeapFile curr_HFile;
	//I still honestly don't know wdf this is needed for :(
	private TransactionId tid;
	
	public HeapFileIterator(TransactionId tid, HeapFile HF){
		this.tid = tid;
		this.curr_HFile = HF;
		this.current_page = 0;
		this.number_pages = HF.numPages();
	}
	//I'm assuming we need to to read pages in, so just use our bufferpool to do so
	public Page readPage(int pagenum) throws DbException, TransactionAbortedException{
		int tablenum = curr_HFile.getId();
		HeapPageId pid = new HeapPageId(tablenum, pagenum);
		Page readPage = Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
		return readPage;
		
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		
		this.current_page = 0;
		this.curr_HPage = (HeapPage)this.readPage(this.current_page);
		this.Tuple_it = curr_HPage.iterator();
		
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(this.Tuple_it == null){
			return false;
		}
		if(this.Tuple_it.hasNext()){
			return true;
		}
		for(int i = this.current_page; i < this.number_pages - 1; i++){
			this.curr_HPage = (HeapPage)this.readPage(i);
			this.Tuple_it = curr_HPage.iterator();
			if(Tuple_it.hasNext()){
				return true;
			}
			
		}
		return false;
		
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if(this.Tuple_it == null){
			throw new NoSuchElementException("Tuple Iterator is empty");
		}
		if(this.Tuple_it.hasNext()){
			return Tuple_it.next();
		}
		throw new NoSuchElementException("Tuple Iterator has run out of items");
		// TODO Auto-generated method stub
	}


	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		this.close();
		this.open();

	}

	@Override
	public void close() {
		this.curr_HFile = null;
		this.Tuple_it = null;
		this.current_page = 0;

	}

}
