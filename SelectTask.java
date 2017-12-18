import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;

public class SelectTask extends Operation implements Callable<ArrayList<ArrayList<Object>>> {
	
	private static AtomicInteger workers;
	private AtomicInteger tableSize;
	private String tableName;
	private String []operations;
	private String condition;
	public static AtomicInteger count;
	public static AtomicInteger max;
	public static AtomicInteger min;
	public static AtomicInteger sum;
	private AtomicInteger col; 
	private HashMap<String, Table> tables;

	public SelectTask (HashMap<String, Table> tables, String tableName, String[] operations, String condition, int numberOfWorkers) {
		
		this.tables = tables;
		workers = new AtomicInteger(numberOfWorkers);
		this.tableName = tableName;
		this.operations = operations;
		this.condition = parseCondition(tables, tableName, condition);
		this.col = new AtomicInteger(getColumnIndex(tables, tableName, condition.substring(0, condition.indexOf(" "))));
		
	}
	@Override
	public ArrayList<ArrayList<Object>> call() throws Exception {
		
			//if tableName is misspelled
			if (tables.get(tableName) == null) {	
				throw new WrongTableName("select -- wrong column argument");
				
			}

			tableSize = new AtomicInteger(tables.get(tableName).table.size());
			ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
			// TODO Auto-generated method stub
			Thread threads[] = new Thread[workers.get()];
			for (int j = 0; j < operations.length; j++) {
				int position = operations[j].indexOf('(');
				//if the command is not an aggregation function,
				//get only the column's name we wish to operate
				String column;
				if (position == -1) {
					column = operations[j];
				}
				else column = operations[j].substring(position + 1, operations[j].length() - 1);
				String function = "";
				if (position != -1) {
					function = operations[j].substring(0, position);
				}
				//check if the column has the same type as the condition / operand
				checkConditionCorrection(tables, tableName, col.get(), condition);
					
				//check if the aggregation function can be applied on the column's type
				int index = getColumnIndex(tables, tableName, column);
				if ( (function.equals("sum") || function.equals("min") || function.equals("max"))
					  &&  tables.get(tableName).columnTypes[index].equals("int") == false) {
					

					throw new DifferentArgumentType("Wrong operand");
				}
					
				count = new AtomicInteger(0);
				sum = new AtomicInteger(0);
				min = new AtomicInteger(Integer.MAX_VALUE);
				max = new AtomicInteger(Integer.MIN_VALUE);
				SelectRunnable[] runnables = new SelectRunnable[workers.get()];
				
				for (int i = 0; i < workers.get(); i++) {
					runnables[i] = new SelectRunnable(tables, i, tableSize.get() / workers.get(), 
										tableName, condition, index, position == -1, col.get());
					threads[i] = new Thread(runnables[i]);
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
					
				ArrayList<Object> operationResult = new ArrayList<Object>();
				if (function.equals("sum")) {
					operationResult.add(sum.get());
				}
				else if (function.equals("min"))
					operationResult.add(min.get());
				else if (function.equals("max"))
					operationResult.add(max.get());
				else if (function.equals("count"))
					operationResult.add(count.get());
				else if (function.equals("avg"))
					operationResult.add(sum.get() / count.get());
				else if (position == -1) {
					for (int i = 0; i < workers.get(); ++i)
						operationResult.addAll(runnables[i].getArray());
				}
				result.add(operationResult);
						
			}
			return result;
		}
	}
