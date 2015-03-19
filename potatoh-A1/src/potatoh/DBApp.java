package potatoh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import potatoh.DBIterator;
import potatoh.DBMetaColumn;
import potatoh.DBAppException;
import potatoh.DBIndex;
import potatoh.DBMetadata;
import potatoh.DBPageData;
import potatoh.DBRow;
import potatoh.DBStatus;

public class DBApp {

	private Properties dbProperties = null;
	private Hashtable<Integer, ArrayList<DBRow>> pageCache = 
			new Hashtable<Integer, ArrayList<DBRow>>();

	public DBApp() {
		init();
	}

	public void init() {
		dbProperties = new Properties();
		try {
			dbProperties.load(new FileInputStream("config/DBApp.properties"));
			if (DBStatus.hasFile()) {
				DBStatus.load();
			}
			if (DBMetadata.hasFile()) {
				DBMetadata.load();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException {
		DBMetadata.addTable(strTableName, htblColNameType, htblColNameRefs,
				strKeyColName);
	}

	public void createIndex(String strTableName, String strColName)
			throws DBAppException {
		DBIndex newIndex = DBStatus.DB_STATUS.createIndex(strTableName,
				strColName);
		DBPageData pageData = DBStatus.DB_STATUS.getCurrentPageCopy(strTableName);
		if (pageData == null)
			throw new DBAppException("No page data for table: " + strTableName
					+ " column:" + strColName);
		Integer currentPage = pageData.pageNo;
		Integer currentRecord = pageData.recCount;
		for (int pageNo = 1; pageNo <= currentPage; pageNo++) {
			indexPage(strTableName, strColName, pageNo, newIndex);
		}
		DBMetadata.DB_META.setIndex(strTableName, strColName);
	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException {
		DBPageData currentPage = DBStatus.DB_STATUS
				.getCurrentPageData(strTableName);
		BufferedWriter bufferedWriter = null;
		File dataFile = null;
		Integer currentRecord = currentPage.recCount;
		try {
			if (currentRecord.intValue() == 0
					|| currentRecord.intValue() == getMaxRecords()) {
				++currentPage.pageNo;
				currentPage.recCount = 1;
				dataFile = getDataFileName(strTableName, currentPage.pageNo);
				if (dataFile.exists()) {
					System.out.println("Data file already exists: "
							+ dataFile.getName());
				} else {
					boolean success = dataFile.createNewFile();
				}
			} else {
				dataFile = getDataFileName(strTableName, currentPage.pageNo);
				++currentPage.recCount;
			}
			bufferedWriter = new BufferedWriter(new FileWriter(dataFile, true));
			String tuple = makeTupleAndIndex(strTableName, htblColNameValue);
			bufferedWriter.append(tuple);
			bufferedWriter.newLine();
			bufferedWriter.flush();

		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException(e);
		} finally {
			try {
				bufferedWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, DBAppException {
		delete_select_fromTable(strTableName, htblColNameValue, strOperator,
				false, null);
	}

	public Iterator selectValueFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, DBAppException {
		DBIterator iter = new DBIterator(); 
		delete_select_fromTable(strTable, htblColNameValue, strOperator, true,iter);
		return iter;

	}

	public Iterator selectRangeFromTable(String strTable,
			Hashtable<String, String> htblColNameRange, String strOperator)
			throws DBEngineException, DBAppException {
		DBIterator iter = new DBIterator(); 
		delete_select_fromTable(strTable, htblColNameRange, strOperator, true,iter);
		return iter;

	}

	public void saveAll() throws DBEngineException {
			DBStatus.save();
			DBMetadata.save();
	}

	
	
	// HELPER METHODS
	
	
	
	public void delete_select_fromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator,
			boolean isSelect, DBIterator iter) throws DBAppException {
		Enumeration<String> keys = htblColNameValue.keys();
		while (keys.hasMoreElements()) {
			String colName = keys.nextElement();
			String value = htblColNameValue.get(colName);
			if (DBStatus.DB_STATUS.hasIndex(strTableName, colName)) {
				DBIndex colIndex = DBStatus.DB_STATUS.getIndex(strTableName,
						colName);
				ArrayList<DBPageData> pages = colIndex.find(value);
				if (pages != null) {
					ArrayList<DBPageData> copies = (ArrayList<DBPageData>) pages
							.clone();
					for (DBPageData page : copies) {
						if ("AND".equals(strOperator))
							if (!isSelect) {
								select_deleteFromPageWithIndexAND(strTableName,
										htblColNameValue, page, null, false);
							} else {
								select_deleteFromPageWithIndexAND(strTableName,
										htblColNameValue, page, iter, true);
							}
						else if (!isSelect) {
							select_deleteFromPageWithIndexOR(strTableName,
									colName, value, page, null, false);
						} else {
							select_deleteFromPageWithIndexOR(strTableName,
									colName, value, page, iter, true);
						}
						if (!isSelect) {
							colIndex.removeFromIndex(value, page);
						} else {
							colIndex.addToIndex(value, page);
						}

					}
				}
			} else {
				if (!isSelect) {
					select_deleteFromTableFullScan(strTableName, colName,
							value, htblColNameValue, strOperator, null, false);
				} else {
					select_deleteFromTableFullScan(strTableName, colName,
							value, htblColNameValue, strOperator, iter, true);
				}
			}
		}
	}

	private void select_deleteFromPageWithIndexOR(String strTableName,
			String colName, String value, DBPageData page, DBIterator iter,
			boolean isSelect) throws DBAppException {
		if (isSelect && pageCache.containsKey(page.pageNo)) {
			select_FromCachedPageWithIndexOR(strTableName, colName, value,
					page, iter);
		} else {
			File dataFile = getDataFileName(strTableName, page.pageNo);
			RandomAccessFile raf = null;
			if (!dataFile.exists()) {
				System.out.println("Data file missing: " + dataFile.getName());
				return;
			}
			try {
				raf = new RandomAccessFile(dataFile, "rw");
				ArrayList<DBRow> pageInCache = new ArrayList<DBRow>();
				pageCache.put(page.pageNo, pageInCache);
				String row = null;
				int cursor = 1;
				while ((row = raf.readLine()) != null) {
					DBRow dataRow = new DBRow(strTableName, row);
					pageInCache.add(dataRow);
					if (cursor == page.recCount) {
						if (!dataRow.hasValue(colName, value)) {
							throw new DBAppException(
									"Missing value to delete for column: "
											+ colName + " value:" + value);
						} else if (!dataRow.isDeleted()) {
							if (!isSelect) {
								markDelete(raf, dataRow);
							} else {
								iter.add(dataRow);
							}
						}
					}
					cursor++;
				}
			} catch (Exception iox) {
				iox.printStackTrace();
			} finally {
				try {
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void select_FromCachedPageWithIndexOR(String strTableName,
			String colName, String value, DBPageData page, DBIterator iter)
			throws DBAppException {
		ArrayList<DBRow> dbRows = pageCache.get(page.pageNo);
		DBRow dataRow = dbRows.get(page.recCount - 1);
		if (!dataRow.hasValue(colName, value)) {
			throw new DBAppException("Missing value to delete for column: "
					+ colName + " value:" + value);
		} else if (!dataRow.isDeleted()) {
			iter.add(dataRow);
		}
	}

	private void select_FromCachedPageWithIndexAND(String strTableName,
			Hashtable<String, String> htblColNameValue, DBPageData page,
			DBIterator iter) throws DBAppException {
		ArrayList<DBRow> dbRows = pageCache.get(page.pageNo);
		DBRow dataRow = dbRows.get(page.recCount - 1);
		if (!dataRow.isDeleted() && dataRow.hasValues(htblColNameValue)) {
			iter.add(dataRow);
		}
	}

	private void select_deleteFromPageWithIndexAND(String strTableName,
			Hashtable<String, String> htblColNameValue, DBPageData page,
			DBIterator iter, boolean isSelect) throws DBAppException {
		if (isSelect && pageCache.containsKey(page.pageNo)) {
			select_FromCachedPageWithIndexAND(strTableName, htblColNameValue,
					page, iter);
		} else {
			File dataFile = getDataFileName(strTableName, page.pageNo);
			RandomAccessFile raf = null;
			if (!dataFile.exists()) {
				System.out.println("Data file missing: " + dataFile.getName());
				return;
			}
			try {
				raf = new RandomAccessFile(dataFile, "rw");
				ArrayList<DBRow> pageInCache = new ArrayList<DBRow>();
				pageCache.put(page.pageNo, pageInCache);
				String row = null;
				int cursor = 1;
				while ((row = raf.readLine()) != null) {
					DBRow dataRow = new DBRow(strTableName, row);
					pageInCache.add(dataRow);
					if (cursor == page.recCount) {
						if (!dataRow.isDeleted()
								&& dataRow.hasValues(htblColNameValue)) {
							if (!isSelect) {
								markDelete(raf, dataRow);
							} else {
								iter.add(dataRow);
							}
						}
					}
					cursor++;
				}
			} catch (Exception iox) {
				iox.printStackTrace();
			} finally {
				try {
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void select_deleteFromTableFullScan(String strTableName,
			String colName, String colValue,
			Hashtable<String, String> htblColNameValue, String strOperator,
			DBIterator iter, boolean isSelect) throws DBAppException {

		DBPageData pageData = DBStatus.DB_STATUS
				.getCurrentPageCopy(strTableName);
		if (pageData == null)
			throw new DBAppException("No page data for table: " + strTableName
					+ " column:" + colName);
		Integer currentPage = pageData.pageNo;
		for (int pageNo = 1; pageNo <= currentPage; pageNo++) {
			if ("AND".equals(strOperator)) {
				if (!isSelect) {
					select_deleteFromPageAND(strTableName, htblColNameValue,
							strOperator, pageNo, null, false);
				} else {
					select_deleteFromPageAND(strTableName, htblColNameValue,
							strOperator, pageNo, iter, true);
				}
			} else {
				if (!isSelect) {
					select_deleteFromPageOR(strTableName, colName, colValue,
							pageNo, null, false);
				} else {
					select_deleteFromPageOR(strTableName, colName, colValue,
							pageNo, iter, true);
				}
			}
		}
	}
	
	private void select_deleteFromPageOR(String strTableName, 
			String colName,String value,Integer pageNo,DBIterator iter, 
			boolean isSelect) {
		File dataFile = getDataFileName(strTableName, pageNo);
		RandomAccessFile raf = null;
		if (!dataFile.exists()) {
			System.out.println("Data file missing: " + dataFile.getName());
			return;
		}
		try {
			raf = new RandomAccessFile(dataFile, "rw");
			String row = null;
			while ((row = raf.readLine()) != null) {
				DBRow dataRow = new DBRow(strTableName, row);
				if (!dataRow.isDeleted() && dataRow.hasValue(colName, value)) {
					if (!isSelect){
						markDelete(raf, dataRow);
					}
					else{
						iter.add(dataRow);
					}

				}
			}
		} catch (Exception iox) {
			iox.printStackTrace();
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void select_deleteFromPageAND(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator,
			Integer pageNo, DBIterator iter, boolean isSelect) {
		File dataFile = getDataFileName(strTableName, pageNo);
		RandomAccessFile raf = null;
		if (!dataFile.exists()) {
			System.out.println("Data file missing: " + dataFile.getName());
			return;
		}
		try {
			raf = new RandomAccessFile(dataFile, "rw");
			String row = null;
			while ((row = raf.readLine()) != null) {
				DBRow dataRow = new DBRow(strTableName, row);
				if (!dataRow.isDeleted()
						&& (dataRow.hasValues(htblColNameValue))) {
					if (!isSelect) {
						markDelete(raf, dataRow);
					} else {
						iter.add(dataRow);
					}
				}
			}
		} catch (Exception iox) {
			iox.printStackTrace();
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void markDelete(RandomAccessFile raf, DBRow dataRow)
			throws IOException {
		long pos = raf.getFilePointer();
		raf.seek(pos - dataRow.getSize() - 1);
		raf.writeByte('-');
		raf.seek(pos);
	}

	public String makeTupleAndIndex(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException {
		String row = "*,";
		ArrayList<DBMetaColumn> metaCols = DBMetadata.DB_META
				.getColumnsMeta(strTableName);
		if (metaCols == null || metaCols.isEmpty())
			throw new DBAppException("Missing metadata for table: "
					+ strTableName);
		for (DBMetaColumn metaCol : metaCols) {
			String colName = metaCol.getColumnName();
			String val = htblColNameValue.get(colName);
			row += val + ",";
			if (metaCol.isKey()
					|| DBStatus.DB_STATUS.hasIndex(strTableName, colName)) {
				DBIndex dbIdx = DBStatus.DB_STATUS.getIndex(strTableName,
						colName);
				DBPageData page = DBStatus.DB_STATUS
						.getCurrentPageCopy(strTableName);
				dbIdx.addToIndex(val, page);
			}
		}

		row = row.substring(0, row.length() - 1);
		return row;
	}

	public Integer getMaxRecords() {
		String max = dbProperties.getProperty("MaximumRowsCountinPage");
		return Integer.valueOf(max);
	}

	public void indexPage(String strTableName, String colName, Integer pageNo,
			DBIndex idx) {
		File dataFile = getDataFileName(strTableName, pageNo);
		BufferedReader bufRead = null;
		if (!dataFile.exists()) {
			System.out.println("Data file missing: " + dataFile.getName());
			return;
		}
		try {
			FileReader fis = new FileReader(dataFile);
			bufRead = new BufferedReader(fis);
			String row = null;
			int recCount = 1;
			while ((row = bufRead.readLine()) != null) {
				DBRow dataRow = new DBRow(strTableName, row);
				String value = dataRow.getValue(colName);
				idx.addToIndex(value, pageNo, recCount++);
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

	private File getDataFileName(String strTableName, Integer pageNo) {
		return new File("data/" + strTableName + "_" + pageNo + ".csv");
	}

}
