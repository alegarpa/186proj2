package simpledb;
import java.io.Serializable;
public class Table implements Serializable {
	private String name;
	private String primaryKeyname;
	private DbFile file;
	public Table(DbFile file, String name, String primaryKey){
		this.name = name;
		this.primaryKeyname = primaryKey;
		this.file = file;
	}
	
	public String getName(){
		return this.name;
	}
	
	public String getpKey(){
		return this.primaryKeyname;
	
	}
	
	public DbFile getContents(){
		return this.file;
	}

}
