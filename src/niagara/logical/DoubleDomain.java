/* $Id: DoubleDomain.java,v 1.1 2007/04/30 19:21:15 vpapad Exp $ */

package niagara.logical;

import niagara.optimizer.colombia.Domain;
import niagara.xmlql_parser.varType;

/**
 * A domain representing Doubles
 */
public class DoubleDomain extends Domain {
    private static DoubleDomain doubleDomain;

    static {
        doubleDomain = new DoubleDomain();
    }
    
    public static DoubleDomain getDomain() {
        return doubleDomain;
    }
    
    // private constructor, IntDomain is a singleton
    private DoubleDomain() {
        super("DoubleDomain");
    }
}
