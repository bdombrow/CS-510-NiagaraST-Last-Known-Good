/* $Id: NullaryOp.java,v 1.1 2002/10/06 23:40:12 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.optimizer.colombia.LogicalProperty;
import niagara.xmlql_parser.op_tree.op;

/**
 * Nullary operators (operators with no inputs)
 */
abstract public class NullaryOp extends op {
    public int getArity() {
        return 0;
    }
}
