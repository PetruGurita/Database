import java.util.HashMap;

public class Operation {
	
	public String parseCondition (HashMap<String, Table> tables, String tableName, String condition) {
		
		String newCondition = "";
		String[] tokens = condition.split(" ");
		//if the condition is not null
		if (tokens.length > 1) {
			
			//check if the tableName has the same type as condition's value
			Table t = tables.get(tableName);
			if (t == null) {
				throw new WrongTableName("parseCondition -- the column name is wrong");
			}
			
			int index = 0;
			for (int i = 0; i < t.columnNames.length; i++) {
				if (t.columnNames[i].equals(tokens[0])) {
					index = i;
					break;
				}
				++index;
			}
			//identify condition's value type
			if (tokens[2].equals("true") || tokens[2].equals("false")) {
				newCondition += "bool";
			}
			else if (tokens[2].matches("-?[0-9]+")) {
				newCondition += "int";
				
			}
			else newCondition += "string";
			if (newCondition.equals(t.columnTypes[index]) == false) {
				throw new DifferentArgumentType("parseCondition -- Column has different type");
			}
			newCondition = newCondition + " " + tokens[1];
			newCondition = newCondition + " " + tokens[2];
		}
		return newCondition;
	}
	
	public int getColumnIndex(HashMap<String, Table> tables, String tableName, String column) {
		
		if (column == null || column.isEmpty()) return -1;
		Table t = tables.get(tableName);
		if (t == null) return -1;
		
		int index = -1;
		for (int i = 0; i < t.columns; ++i) {
			if (t.columnNames[i].equals(column)) {
				index = i;
				break;
			}
		}
		return index;
	}
	public void checkConditionCorrection(HashMap<String, Table> tables, String tableName, int column, String condition) {
		
		if (condition == null || condition.isEmpty()) return;
		Table t = tables.get(tableName);

		if (column == -1) {
			
			//throw exception
			throw new WrongTableName("checkConditionCorrection -- wrong column name");
			
		}
		
		String[] split = condition.split(" ");
		if (t.columnTypes[column].equals(split[0]) == false) {
			
			//throw exception

			throw new DifferentArgumentType("checkConditionCorrection -- Column has different type");
		}
	}
	
	public boolean checkCondition(String condition, Object o, String type) {
		
		if (condition == null || condition.isEmpty() == true) return true;
		String[] split = condition.split(" ");
		
		if(split[0].equals("string") || split[0].equals("bool")) {
			return split[2].equals(o.toString());
		}
		int value1 = Integer.parseInt(split[2]);
		int value2 = (Integer) o;
		if (split[1].equals("==")) return value1 == value2;
		else if (split[1].equals(">")) return value2 > value1;
		else return value2 < value1;
	}
	
}
