/* $Id: NullaryOp.java,v 1.2 2003/02/25 06:13:16 vpapad Exp $ */
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
