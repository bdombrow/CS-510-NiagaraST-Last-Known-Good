/**
 * MTException.java
 * Created: June 25, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

package niagara.physical;

/**
 * <code> MTException </code>  An error generated
 * by the merge tree - is used most for errors during
 * translation of XML Document representing the merge template
 * to an in-memory MergeTree
 * @see Exception
 */

public class MTException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public MTException() {  
        super("Merge Tree Exception:");
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public MTException(String msg) 
    {
        super("Merge Tree Exception:"+msg);
    }
    
} 
