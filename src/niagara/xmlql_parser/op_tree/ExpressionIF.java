
package niagara.xmlql_parser.op_tree;


/**
 * Classes to be used with ExpressionOp must
 * implement the <code>ExpressionIF</code> interface.
 *
 * @version 1.0
 */

import niagara.utils.StreamTupleElement;
import org.w3c.dom.*;

public interface ExpressionIF {
    Node processTuple(StreamTupleElement ste);
}