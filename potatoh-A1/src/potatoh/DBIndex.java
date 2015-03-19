package potatoh;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.StringComparator;

public class DBIndex implements Serializable {

	private static final long serialVersionUID = 1L;
	private RecordManager recman;
	BTree idxBTree = null;

	public DBIndex() {
		init();
	}

	private void init() {
		try {
			recman = RecordManagerFactory.createRecordManager("data/DB_BTREE");
			idxBTree = BTree.createInstance(recman, new StringComparator());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void removeFromIndex(Object value, DBPageData page) {
		Object result;
		try {
			result = idxBTree.find(value);
			if (result != null) {
				ArrayList<DBPageData> pageArray = (ArrayList<DBPageData>) result;
				if (pageArray.size() == 1) {
					idxBTree.remove(value);
				} else {
					pageArray.remove(page);
				}
				recman.commit();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<DBPageData> find(Object value) {
		ArrayList<DBPageData> res = null;
		try {
			res = (ArrayList<DBPageData>) idxBTree.find(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}

	public void addToIndex(Object value, Integer pageNo, Integer recCount) {
		DBPageData page = new DBPageData(pageNo, recCount);
		addToIndex(value, page);
	}

	public void addToIndex(Object value, DBPageData page) {
		try {
			Object result = idxBTree.find(value);
			if (result == null) {
				ArrayList<DBPageData> pageArray = new ArrayList<DBPageData>();
				pageArray.add(page);
				idxBTree.insert(value, pageArray, true);
			} else {
				ArrayList<DBPageData> pageArray = (ArrayList<DBPageData>) result;
				if (!pageArray.contains(page))
					pageArray.add(page);
			}
			recman.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		idxBTree.writeExternal(oos);
	}

	private void readObject(ObjectInputStream in) throws IOException {
		try {
			init();
			idxBTree.readExternal(in);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		return "DBIndex with BTree size: " + idxBTree.size();
	}

}
