package niagara.utils;

/**
 * CUException.java
 * Created: August 3, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> CUException </code>  An error generated
 * during the execution of a CUtil function - these
 * are primarily IO (file not found/bad url) and parse
 * errors
 *
 * @see Exception
 */

public class CUException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public CUException() {  
        super();
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public CUException(String msg) 
    {
        super(msg);
    }
    
} 
