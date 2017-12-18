import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table {
	
	public ArrayList<ArrayList<Object>> table;
	public String[] columnNames;
	public String[] columnTypes;
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    public final Lock r = rwl.readLock();
    public final Lock w = rwl.writeLock();
    public ReentrantLock transaction = new ReentrantLock(); 
	int columns;
	public Table(String[] columnNames, String[] columnTypes) {
		
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		table = new ArrayList<ArrayList<Object>>();
		columns = columnNames.length;
	}
}


