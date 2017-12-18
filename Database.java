import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
public class Database implements MyDatabase {
	
	private static int numberOfThreads = 0;
	public HashMap<String, Table> tables = new HashMap<String, Table>();
	private ExecutorService tpe;
	
	@Override
	public void initDb(int numWorkerThreads) {
		// TODO Auto-generated method stub
		numberOfThreads = numWorkerThreads;
		tpe = Executors.newFixedThreadPool(numberOfThreads);
	}

	@Override
	public void stopDb() {
		// TODO Auto-generated method stub
		tpe.shutdown();
	}

	@Override
	public void createTable(String tableName, String[] columnNames, String[] columnTypes) {
		// TODO Auto-generated method stub
		tables.put(tableName, new Table(columnNames, columnTypes));
	}

	@Override
	public ArrayList<ArrayList<Object>>select(String tableName, String[] operations, String condition) {
		

		if (tables.get(tableName).transaction.isHeldByCurrentThread()) {
			tables.get(tableName).r.lock();
			Callable<ArrayList<ArrayList<Object>>> callable = new SelectTask(tables, tableName, operations,
																			condition, numberOfThreads);
			
			Future<ArrayList<ArrayList<Object>>> future = tpe.submit(callable);
			
			ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
			try {
				result = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			tables.get(tableName).r.unlock();
			return result;
			
		}
		else {
			tables.get(tableName).transaction.lock();
			tables.get(tableName).transaction.unlock();
			tables.get(tableName).r.lock();

			Callable<ArrayList<ArrayList<Object>>> callable = new SelectTask(tables, tableName, operations, condition, numberOfThreads);
			
			Future<ArrayList<ArrayList<Object>>> future = tpe.submit(callable);
			
			ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
			try {
				result = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			tables.get(tableName).r.unlock();
			return result;
			
		}
			
	}
	@Override
	public void update(String tableName, ArrayList<Object> values, String condition) {
		
		
		// TODO Auto-generated method stub
		if (tables.get(tableName).transaction.isHeldByCurrentThread()) {
			tables.get(tableName).w.lock();
			Callable<HashMap<String, Table>> callable = new UpdateTask(tables, tableName, values, condition, numberOfThreads);
	
			Future<HashMap<String, Table>> future = tpe.submit(callable);
	    		   		
			try {
				tables = future.get();
			} catch (InterruptedException | ExecutionException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
			}
			tables.get(tableName).w.unlock();
		}
		else {
			tables.get(tableName).transaction.lock();
			tables.get(tableName).transaction.unlock();
			tables.get(tableName).w.lock();   

			Callable<HashMap<String, Table>> callable = new UpdateTask(tables, tableName, values, condition, numberOfThreads);
			Future<HashMap<String, Table>> future = tpe.submit(callable);
	    		   		
			try {
				tables = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			
			tables.get(tableName).w.unlock();
		}
	}

	@Override
	public void insert(String tableName, ArrayList<Object> values) {
		
		// TODO Auto-generated method stub
		if (tables.get(tableName).transaction.isHeldByCurrentThread()) {
			tables.get(tableName).w.lock();
			tables.get(tableName).table.add(values);
			tables.get(tableName).w.unlock();
		}
		else {
			tables.get(tableName).transaction.lock();
			tables.get(tableName).transaction.unlock();
			tables.get(tableName).w.lock();
			tables.get(tableName).table.add(values);
			tables.get(tableName).w.unlock();
		}
	}
	@Override
	public void startTransaction(String tableName) {
		// TODO Auto-generated method stub
		tables.get(tableName).transaction.lock();
		
		tables.get(tableName).w.lock();
		tables.get(tableName).w.unlock();
	}

	@Override
	public void endTransaction(String tableName) {
		// TODO Auto-generated method stub
		tables.get(tableName).transaction.unlock();
		
	}

}