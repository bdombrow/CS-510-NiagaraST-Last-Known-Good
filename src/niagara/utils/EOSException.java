package niagara.utils;

/**
 * EOSException.java
 * Created: September 9, 2002 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * A EOSException is thrown to indicate End Of Stream
 */

public class EOSException extends Exception {
  
    public EOSException() {  
        super("End Of Stream");
    }
    
    public EOSException(String msg) {
        super("End Of Stream:"+msg);
        
    }
    
}
