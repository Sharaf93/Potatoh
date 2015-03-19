package potatoh;

import java.util.ArrayList;
import java.util.Iterator;

public class DBIterator implements Iterator {

	private ArrayList<DBRow> rows = new  ArrayList<DBRow>();
	int cursor = 0;
	
	public DBIterator() {		
	}

	public void add(DBRow row) {
		rows.add(row);
	}

	public boolean hasNext() {
		if (rows.isEmpty()){
			return false;
		}
		return cursor != rows.size();
	}

	public DBRow next() {
		if (rows.isEmpty()){
			return null;
		}
		return rows.get(cursor++);
	}

	@Override
	public void remove() {
	}

}