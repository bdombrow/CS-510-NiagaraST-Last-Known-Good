/* $Id: NullaryOp.java,v 1.3 2003/07/03 19:39:02 tufte Exp $ */
package niagara.logical;

import niagara.xmlql_parser.op_tree.op;

/**
 * Nullary operators (operators with no inputs)
 */
abstract public class NullaryOp extends op {
    public int getArity() {
        return 0;
    }    
}
