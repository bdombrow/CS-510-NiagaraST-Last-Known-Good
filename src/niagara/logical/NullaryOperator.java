/* $Id: NullaryOperator.java,v 1.1 2003/12/24 02:08:27 vpapad Exp $ */
package niagara.logical;


/**
 * Nullary operators (operators with no inputs)
 */
abstract public class NullaryOperator extends LogicalOperator {
    public int getArity() {
        return 0;
    }    
}
