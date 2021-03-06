package umbc.hebeler.phd.bigdata;

import java.util.ArrayList;

import com.mongodb.DBObject;

public class DBStructure {
	private String dbName = null;
	private String tableName = null;
	private long rowCount = 0;
	public long getRowCount() {
		return rowCount;
	}
	public void setRowCount(long rowCount) {
		this.rowCount = rowCount;
	}

	private ArrayList<Object> structures = new ArrayList<Object>();
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public ArrayList<Object> getStructures() {
		return structures;
	}
	public void setStructures(ArrayList<Object> structures) {
		this.structures = structures;
	}
	
	public void add(Object o){
		System.out.println("Adding Structure");
		structures.add(o);
	}

}
