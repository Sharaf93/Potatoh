package potatoh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class DBMetadata {

	public static DBMetadata DB_META = new DBMetadata();
	static String metadataFileName = "data/metadata.csv";
	private Hashtable<String, ArrayList<DBMetaColumn> > metadata = 
			new Hashtable<String,  ArrayList<DBMetaColumn> >();

	public ArrayList<DBMetaColumn> getColumnsMeta(String tableName){
		return metadata.get(tableName);
	}
	
	public ArrayList<String> getMetaAsRows() {
		Enumeration keys = metadata.keys();
		ArrayList<String> list = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			ArrayList<DBMetaColumn> metaCols = getColumnsMeta(key);
			for ( DBMetaColumn col: metaCols) {
				list.add(col.toString());
			}
		}
			
		return list;
	}

	public void setIndex(String tableName, String colName) {
		ArrayList<DBMetaColumn> metaCols = getColumnsMeta(tableName);
		for ( DBMetaColumn col: metaCols) {
			if (col.equals(colName)) {
				col.setIndex();
			}
		}
	}

	public static boolean hasFile() {
		File metaFile = new File(metadataFileName);
		return metaFile.exists();
	}

	public static void save() {
		BufferedWriter bufferedWriter = null;	   
		try {
			File metaFile = new File(metadataFileName);
			if (! metaFile.exists()) {
				boolean success = metaFile.createNewFile();
			}
			bufferedWriter = new BufferedWriter(new FileWriter(metaFile));
			ArrayList<String> rows = DB_META.getMetaAsRows();
			for( String row : rows) {
				bufferedWriter.write(row);
				bufferedWriter.newLine();
			}
			bufferedWriter.flush();
		} catch (IOException iox) {
			iox.printStackTrace();
		} finally {
			try {
				bufferedWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}

	public static void load() {
		BufferedReader bufRead = null;
		if (! hasFile()) {
			System.out.println("Cannot load. File was not created for: " + metadataFileName);	
			return;
		}
		try {
			FileReader fis = new FileReader(metadataFileName);
			bufRead = new BufferedReader(fis);
			DB_META = new DBMetadata();
			String tableName = null;
			ArrayList<DBMetaColumn> colArray = new ArrayList<DBMetaColumn>();
			String row = null;
			while ((row = bufRead.readLine()) != null) { 
				StringTokenizer sToken = new StringTokenizer(row,",");
				String[] rowTokens = new String[6];
				int j = 0;
			    while (sToken.hasMoreTokens()) {
			    	rowTokens[j++] = sToken.nextToken();
			    }
			    String rowTable = rowTokens[0];
			    if (!rowTable.equals(tableName)) {
			    	tableName = rowTable;
			    	colArray = new ArrayList<DBMetaColumn>();
			    	DB_META.metadata.put(tableName, colArray);
			    }
			    DBMetaColumn dmc = new DBMetaColumn(rowTokens);
			    colArray.add(dmc);
			    
			}
		} catch (Exception iox) {
			iox.printStackTrace();
		} finally {
			try {
				bufRead.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static File getFile() {
		return new File(metadataFileName);
	}

	public static void addTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException {

		ArrayList<DBMetaColumn> colArray = new ArrayList<DBMetaColumn>();
		Enumeration<String> keys = htblColNameType.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			String type = htblColNameType.get(key);
			String ref="null";
			if (htblColNameRefs!=null && htblColNameRefs.containsKey(key)){
				 ref=htblColNameRefs.get(key);
			}
			String isKey = "False";
			if (key.equals(strKeyColName)) {
				isKey = "True";
			}
			String[] row = {strTableName , key,type, isKey ,isKey, ref};
		    DBMetaColumn dmc = new DBMetaColumn(row);
		    colArray.add(dmc);
		}
    	DB_META.metadata.put(strTableName, colArray);

	}

}