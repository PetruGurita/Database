public class WrongTableName extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WrongTableName(){
        super();
    }

    public WrongTableName(String message){
        super(message);
    }
}