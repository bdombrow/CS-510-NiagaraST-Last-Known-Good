package niagara.utils;

/**
 * ParseException.java
 * Created: August 3, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> ParseException </code>  An error generated
 * during by the parsing of an xml file
 *
 * @see Exception
 */

public class ParseException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public ParseException() {  
        super();
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public ParseException(String msg) 
    {
        super(msg);
    }
    
} 
