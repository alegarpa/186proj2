package simpledb;

import java.io.*;
import java.util.*;

/**
* HeapFile is an implementation of a DbFile that stores a collection of tuples
* in no particular order. Tuples are stored on pages, each of which is a fixed
* size, and the file is simply a collection of those pages. HeapFile works
* closely with HeapPage. The format of HeapPages is described in the HeapPage
* constructor.
*
* @see simpledb.HeapPage#HeapPage
* @author Sam Madden
*/
public class HeapFile implements DbFile {

    /**
* Constructs a heap file backed by the specified file.
*
* @param f
* the file that stores the on-disk backing store for this heap
* file.
*/
    private File file;
    private TupleDesc tupledesc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupledesc = td;

    }

    /**
* Returns the File backing this HeapFile on disk.
*
* @return the File backing this HeapFile on disk.
*/
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
* Returns an ID uniquely identifying this HeapFile. Implementation note:
* you will need to generate this tableid somewhere ensure that each
* HeapFile has a "unique id," and that you always return the same value for
* a particular HeapFile. We suggest hashing the absolute file name of the
* file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
*
* @return an ID uniquely identifying this HeapFile.
*/
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
* Returns the TupleDesc of the table stored in this DbFile.
*
* @return TupleDesc of this DbFile.
*/
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupledesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if(pid.getTableId() == getId()){
         try{        
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                raf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
                byte[] buffer = new byte[BufferPool.PAGE_SIZE];
                raf.read(buffer, 0, BufferPool.PAGE_SIZE);
                raf.close();
                return new HeapPage((HeapPageId)pid, buffer);
         }
         catch (IOException e){
                e.printStackTrace();
         }
        }
        throw new IllegalArgumentException();
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
            int pageNum = page.getId().pageNumber();
            //array of data I want
            byte[] writeData = new byte[BufferPool.PAGE_SIZE];
            writeData = page.getPageData();
            //we just need a new file to write to, and that should be fine
            //I hope... - _ -...
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            f.seek(pageNum * BufferPool.PAGE_SIZE);
            f.write(writeData, 0, BufferPool.PAGE_SIZE);
            f.close();
            page.markDirty(false, null);
    }

    /**
* Returns the number of pages in this HeapFile.
*/
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length()/(double)BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
            ArrayList<Page> mPages = new ArrayList<Page>();
            for(int i = 0; i < this.numPages(); i++){
                    PageId currPID = new HeapPageId(this.getId(), i);
                    HeapPage currPage = (HeapPage) Database.getBufferPool().getPage(tid, currPID, Permissions.READ_WRITE);
                    int numSlots =currPage.getNumEmptySlots();
                    if(numSlots >= 1){
                            currPage.insertTuple(t);
                            currPage.markDirty(true, tid);
                            mPages.add(currPage);
                            return mPages;
                    }
            }
       int newPageNum = numPages();
       HeapPageId newPageId = new HeapPageId(this.getId(), newPageNum);
       HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
       newPage.insertTuple(t);
       mPages.add(newPage);
       RandomAccessFile f = new RandomAccessFile(file, "rw");
       f.seek(newPageNum *BufferPool.PAGE_SIZE);
       f.write(newPage.getPageData(), 0, BufferPool.PAGE_SIZE);
       f.close();
       return mPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
            //BufferPool pool = Database.getBufferPool();
        RecordId rid = t.getRecordId();
        HeapPage currPage = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        // maybe have a list of free pages
        //markFree(page.getId().pageNumber(), page.hasFreeSlots());
        currPage.deleteTuple(t);
        //do I have to mark it as dirty?
        currPage.markDirty(true, tid);
        return currPage;
}

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator{
        
        private TransactionId tid;
        private Iterator<Tuple> currentPageIterator;
        private int pageNumber;

        public HeapFileIterator(TransactionId tid){
         this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
         pageNumber = 0;
         HeapPageId id = new HeapPageId(getId(), pageNumber);
         HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, id, Permissions.READ_ONLY);
         currentPageIterator = page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
         if(currentPageIterator == null) return false;
         if(currentPageIterator.hasNext()) return true;
         if(pageNumber < numPages()-1){
                pageNumber++;
                HeapPageId id = new HeapPageId(getId(), pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, id, Permissions.READ_ONLY);
                if(page.getNumEmptySlots() == page.numSlots){
                	return false;
                }
                currentPageIterator = page.iterator();
                return true;
         }
         return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
         if(currentPageIterator == null) throw new NoSuchElementException("need to call open first");
         if(currentPageIterator.hasNext()){
                Tuple t = currentPageIterator.next();
                if(t != null)
                 return t;
         }
         throw new NoSuchElementException("no more tuples in this file");
        }

        public void close(){
         currentPageIterator = null;
         pageNumber = 0;
        }

        public void rewind() throws DbException, TransactionAbortedException {
         close();
         open();
        }


    }

}

