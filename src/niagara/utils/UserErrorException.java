package niagara.utils;

/**
 * $Id: UserErrorException.java,v 1.2 2002/10/24 23:26:37 vpapad Exp $
 * UserErrorException.java
 * Created: March 30, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * A UserErrorException should be thrown when the server
 * detects an error made by the user - bad url for example.
 * This error should be reported back to the client, the
 * server should not crash
 */

public class UserErrorException extends RuntimeException {
  
    /**
     * constructor
     * @see Exception
     */
    public UserErrorException() {  
        super("User Error:");
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public UserErrorException(String msg) {
        super("User Error:"+msg);
        
    }
    
}
