package potatoh;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBStatus implements Serializable {

	private static final long serialVersionUID = 1L;
	private Hashtable<String, DBPageData> pagesDictionary = 
			new Hashtable<String, DBPageData>();
	private Hashtable<String, DBIndex> indexesDictionary = new Hashtable<String, DBIndex>();
	public static DBStatus DB_STATUS = new DBStatus();
	static String statusFileName = "data/DbStatus.properties";
	
	public DBPageData getCurrentPageData(String tableName) {
		if(! pagesDictionary.containsKey(tableName)){
			pagesDictionary.put(tableName, new DBPageData());
		}
		return  pagesDictionary.get(tableName);
	}
	
	public DBPageData getCurrentPageCopy(String tableName) {
		DBPageData page = pagesDictionary.get(tableName);
		if (page == null) return null;
		return new DBPageData(page.pageNo, page.recCount);
	}

	public void showCurrentPages() {
		if (pagesDictionary.isEmpty()) {
			System.out.println("Empty pagesDictionary! ");
		} else {
			Enumeration<String> keys = pagesDictionary.keys();
			while (keys.hasMoreElements()){
				String key = keys.nextElement();
				DBPageData page = pagesDictionary.get(key);
				System.out.println("Table: "+ key + " has: "+page);
			}
		}
	}

	public void showIndexes() {
		if (indexesDictionary.isEmpty()) {
			System.out.println("Empty indexesDictionary! ");
		} else {
			Enumeration<String> keys = indexesDictionary.keys();
			while (keys.hasMoreElements()){
				String key = keys.nextElement();
				DBIndex idx = indexesDictionary.get(key);
				System.out.println("Table: "+ key + " has: "+idx);
			}
		}
	}

	public void showAll() {
		showCurrentPages();
		showIndexes();
	}

	public DBIndex getIndex(String table, String col) {
		String key = table+"."+col;
		if ( ! indexesDictionary.containsKey(key)) {
			indexesDictionary.put(key, new DBIndex());	
		}
		return indexesDictionary.get(key);		
	}
	
	public DBIndex createIndex(String table, String col) {
		String key = table+"."+col;
		indexesDictionary.put(key, new DBIndex());	
		return indexesDictionary.get(key);		
	}

	public boolean hasIndex(String table, String col) {
		String key = table+"."+col;
		return indexesDictionary.containsKey(key);	
	}

	public static boolean hasFile() {
		File metaFile = new File(statusFileName);
		return metaFile.exists();
	}

	public static void save() {
		ObjectOutputStream oos = null;
		try {
			
			File statusFile = new File(statusFileName);
			if (! statusFile.exists()) {
				boolean success = statusFile.createNewFile();
				System.out.println("Status file was created with success: " + success);
			}
			FileOutputStream fos = new FileOutputStream(statusFileName);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(DB_STATUS);
		} catch (IOException iox) {
			iox.printStackTrace();
		} finally {
			try {
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}

	public static void load() {
		ObjectInputStream ois = null;
		if (! hasFile()) {
			System.out.println("Cannot load. File was not created for: " + statusFileName);	
			return;
		}
		try {
			FileInputStream fis = new FileInputStream(statusFileName);
			ois = new ObjectInputStream(fis);
			DB_STATUS = (DBStatus)ois.readObject();
		} catch (Exception iox) {
			iox.printStackTrace();
		} finally {
			try {
				ois.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}