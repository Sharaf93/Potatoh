package potatoh;

public class DBAppException extends Exception {

	public DBAppException(String message){
		super(message);
	}
	
	public DBAppException(Exception ex){
		super(ex);
	}

}
