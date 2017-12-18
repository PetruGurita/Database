import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
public class SelectRunnable extends Operation implements Runnable{
	
	private AtomicInteger length;
	private AtomicInteger id;
	private String condition;
	private AtomicInteger index;
	private AtomicBoolean insert;
	private String tableName;
	private Vector<Object> vector;
	private HashMap<String, Table> tables;
	private AtomicInteger col;
	
	public SelectRunnable(HashMap<String, Table> tables, int id, int length, String tableName, String condition, int index, boolean insert,
						  int col) {
		
		this.tables = tables;
		this.length = new AtomicInteger(length);
		this.id = new AtomicInteger(id);
		this.condition = condition;
		this.index = new AtomicInteger(index);
		this.insert = new AtomicBoolean(insert);
		this.tableName = tableName;
		this.vector = new Vector<Object>();
		this.col = new AtomicInteger(col);
		
	}
	public void run() {
		// TODO Auto-generated method stub
		
		int start = length.get() * id.get();
		int stop = length.get() * (id.get() + 1);
		for(int i = start; i < stop; ++i) {
			Object o = tables.get(tableName).table.get(i).get(index.get());
			Object o2 = tables.get(tableName).table.get(i).get(col.get());
			boolean check = checkCondition(condition, o2, tables.get(tableName).columnTypes[index.get()]);
			if (check == true) {
				if (insert.get() == true) vector.add(o);
				int getCount = SelectTask.count.getAndIncrement();
				++getCount;
				SelectTask.count.set(getCount);
				if (tables.get(tableName).columnTypes[index.get()].equals("int")) {
					int getMax = SelectTask.max.get();
					SelectTask.max.set((getMax) < ((Integer) o) ? ((Integer) o) : (getMax));
					
					int getMin = SelectTask.min.get();
					SelectTask.min.set((getMin) > ((Integer) o) ? ((Integer) o) : (getMin));
					
					SelectTask.sum.getAndAdd((Integer) o);
				}
			}
		}
	}
	ArrayList<Object> getArray() {
		ArrayList<Object> list = new ArrayList<Object>(vector);
		return list;
	}

}
