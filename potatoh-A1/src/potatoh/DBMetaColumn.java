package potatoh;

public class DBMetaColumn {

	String[] metaValues;

	public void setIndex() {
		metaValues[4] = "True";
	}
	
	public DBMetaColumn(String[] meta) {
		metaValues = meta;
	}
	
	public String getTableName() {
		return metaValues[0];
	}

	public String getColumnName() {
		return metaValues[1];
	}
	
	public String getColumnType() {
		return metaValues[2];
	}
	
	public boolean isKey() {
		return "True".equals(metaValues[3]);
	}
	
	public boolean hasIndex() {
		return "True".equals(metaValues[4]);
	}

	public String getColumnRef() {
		return metaValues[5];
	}

	public String toString() {
		return metaValues[0]+","+metaValues[1]+","+metaValues[2]+","+metaValues[3]+","+metaValues[4]+","+metaValues[5];
	}
}