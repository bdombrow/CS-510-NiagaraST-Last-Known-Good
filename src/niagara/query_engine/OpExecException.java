package niagara.query_engine;

/**
 * OpExecException.java
 * Created: March 30, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> OpExecException </code>  An error generated
 * during operator execution. 
 * @see Exception
 */

public class OpExecException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public OpExecException() {  
        super("Operator Execution Exception:");
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public OpExecException(String msg) 
    {
        super("Operator Execution Exception:"+msg);
    }
    
} 
