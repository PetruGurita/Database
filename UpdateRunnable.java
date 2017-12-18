import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateRunnable extends Operation implements Runnable{
	
	private AtomicInteger length;
	private AtomicInteger id;
	private String condition;
	private AtomicInteger index;
	private String tableName;
	private Vector<Object> values;
	private HashMap<String, Table> tables;
	public UpdateRunnable(HashMap<String, Table> tables, int id, int length, String tableName, String condition, int index, Vector<Object> values) {
		
		this.tables = tables;
		this.length = new AtomicInteger(length);
		this.id = new AtomicInteger(id);
		this.condition = condition;
		this.index = new AtomicInteger(index);
		this.tableName = tableName;
		this.values = new Vector<Object>(values);
		
	}
	public void run() {
		// TODO Auto-generated method stub
		Table t = tables.get(tableName);
		int start = length.get() * id.get();
		int stop = length.get() * (id.get() + 1);
			for(int i = start; i < stop; ++i) {
				Object o = t.table.get(i).get(index.get());
				boolean check;
				if (index.get() == -1) check = true;
				else check = checkCondition(condition, o, t.columnTypes[index.get()]);
			
				if (check == true) {
					for (int j = 0; j < t.columns; ++j)
						tables.get(tableName).table.get(i).set(j, values.get(j));
				}
			}
	}
	public HashMap<String, Table> getTable() {
		return tables;
	}
}
