package potatoh;

import java.io.Serializable;
import potatoh.DBPageData;

public class DBPageData implements Serializable {

	private static final long serialVersionUID = 1L;
	public Integer pageNo = 0;
	public Integer recCount = 0;
	
	public DBPageData() {		
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pageNo == null) ? 0 : pageNo.hashCode());
		result = prime * result
				+ ((recCount == null) ? 0 : recCount.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBPageData other = (DBPageData) obj;
		if (pageNo == null) {
			if (other.pageNo != null)
				return false;
		} else if (!pageNo.equals(other.pageNo))
			return false;
		if (recCount == null) {
			if (other.recCount != null)
				return false;
		} else if (!recCount.equals(other.recCount))
			return false;
		return true;
	}


	public DBPageData(Integer page, Integer recCnt){
		pageNo = page;
		recCount = recCnt;
	}
	
	public String toString() {
		return "Page: " + pageNo + " with rec count: " + recCount;
	}
}