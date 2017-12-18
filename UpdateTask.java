import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
public class UpdateTask extends Operation implements Callable<HashMap<String, Table>> {
	
	private HashMap<String, Table> tables;
	private static AtomicInteger workers;
	private AtomicInteger tableSize;
	private String tableName;
	private String condition;
	private Vector<Object> values;
	private String columnName;
	private AtomicInteger col;
	public static CyclicBarrier barrier;
	
	public UpdateTask (HashMap<String, Table> tables, String tableName, ArrayList<Object> values, String condition, int numberOfWorkers) {
		this.tables = tables;
		tableSize = new AtomicInteger(tables.get(tableName).table.size());
		workers = new AtomicInteger(numberOfWorkers);
		this.tableName = tableName;
		this.values =  new Vector<Object>(values);
		
		//check if the column has the specified type
		//save the column's name
		columnName = condition.substring(0, condition.indexOf(" "));
		this.col = new AtomicInteger(getColumnIndex(tables, tableName, condition.substring(0, condition.indexOf(" "))));
		this.condition = parseCondition(tables,tableName, condition);
		barrier = new CyclicBarrier(numberOfWorkers);
	}
	@Override
	public HashMap<String, Table> call() throws Exception {
			
		// TODO Auto-generated method stub
		Thread threads[] = new Thread[workers.get()];
				
		//if tableName is misspelled
		if (tables.get(tableName) == null) {
			
			throw new WrongTableName("update -- wrong column argument");
		}
		//check if the column has the same type as the condition / operand

		checkConditionCorrection(tables, tableName, col.get(), condition);
					
		//check if the values are compatible with table's content
		for (int i = 0; i < tables.get(tableName).columns; ++i) {
			if ((tables.get(tableName).columnTypes[i].equals("int") &&
					values.get(i).getClass() != Integer.class) ||
				(tables.get(tableName).columnTypes[i].equals("string") &&
							values.get(i).getClass() != String.class) ||
				(tables.get(tableName).columnTypes[i].equals("bool") &&
							values.get(i).getClass() != Boolean.class)) {
							
				//throw exception

				throw new DifferentArgumentType("Update - [values] have incorrect type values");
			}		
		}
					
		int index = getColumnIndex(tables, tableName, columnName);
		UpdateRunnable[] updateRunnable = new UpdateRunnable[workers.get()];
		for (int i = 0; i < workers.get(); i++) {
			updateRunnable[i] = new UpdateRunnable(tables, i, tableSize.get() / workers.get(), tableName, condition, index, values);
			threads[i] = new Thread(updateRunnable[i]);
		}
		for (int i = 0; i < workers.get(); i++)
			threads[i].start();
				
		for (int i = 0; i < workers.get(); i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
					
		return updateRunnable[0].getTable();	
    }
}
	

