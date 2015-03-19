package potatoh;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class DBRow {

		private ArrayList<DBMetaColumn>  metaColArray;
		private ArrayList<String> values = new ArrayList<String>();
		long size = 0;
		
		public DBRow(String strTableName, String row){
			metaColArray = DBMetadata.DB_META.getColumnsMeta(strTableName);
			StringTokenizer sToken = new StringTokenizer(row,",");
		    while (sToken.hasMoreTokens()) {
		    	String current = sToken.nextToken();
		    	values.add(current);
		    	size += current.length();
		    	size +=1;
		    }
		    --size;
		}

		public String getValue (String colName) {		
			int j = 0;
			for (DBMetaColumn metaCol : metaColArray) {
				if (metaCol.getColumnName().equals(colName)) {
					break;
				}
				j++;
			}
			return values.get(j+1);
		}

		public boolean hasValues (Hashtable<String, String> htblColNameValue) {

			Enumeration<String> keys = htblColNameValue.keys();
			while (keys.hasMoreElements()){
				String colName = keys.nextElement();
				String value = htblColNameValue.get(colName);
				if (! hasValue ( colName,  value)) {
					return false;
				}
			}
			
			return true;
		}

		public boolean hasValue (String colName, String value) {
			
			int j = 0;
			for (DBMetaColumn metaCol : metaColArray) {
				if (metaCol.getColumnName().equals(colName)) {
					break;
				}
				j++;
			}
			String found= values.get(j+1);
			return found.equals(value);
		}

		boolean isDeleted() {
			return values.get(0).equals("-");
		}

		public String toString() {
			return "Data row: " + values;
		}

		public long getSize() {
			return size;
		}
}