package niagara.utils.nitree;

/**
 * NITreeException.java
 * Created: June 22, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> NITreeException </code>  An error generated
 * by execution of a function in a NITree
 * @see Exception
 */

public class NITreeException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public NITreeException() {  
        super("NITree Exception:");
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public NITreeException(String msg) 
    {
        super("NITree Exception:"+msg);
    }
    
} 
