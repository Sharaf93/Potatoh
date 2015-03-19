package potatoh;

public class DBEngineException extends Exception {

	public DBEngineException(String message){
		super(message);
	}
	
	public DBEngineException(Exception ex){
		super(ex);
	}

}