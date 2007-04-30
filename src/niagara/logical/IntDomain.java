/* $Id: IntDomain.java,v 1.1 2007/04/30 19:21:16 vpapad Exp $ */

package niagara.logical;

import niagara.optimizer.colombia.Domain;
import niagara.xmlql_parser.varType;

/**
 * A domain representing Integers
 */
public class IntDomain extends Domain {
    private static IntDomain intDomain;

    static {
        intDomain = new IntDomain();
    }
    
    public static IntDomain getDomain() {
        return intDomain;
    }
    
    // private constructor, IntDomain is a singleton
    private IntDomain() {
        super("IntDomain");
    }
}
