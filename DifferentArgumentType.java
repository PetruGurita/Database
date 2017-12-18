public class DifferentArgumentType extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DifferentArgumentType(){
        super();
    }

    public DifferentArgumentType(String message){
        super(message);
    }
}