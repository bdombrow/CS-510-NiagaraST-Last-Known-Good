/* $Id$ */
package niagara.optimizer;

import niagara.optimizer.colombia.*;
import niagara.utils.PEException;

/** ConsolidateMe is a pseudo-physical op that forces Colombia to 
 * optimize its input groups */
public class ConsolidateMe extends PhysicalOp {
    private int arity;
    
    public ConsolidateMe(int arity) {
        this.arity = arity;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty FindPhysProp(PhysicalProperty[] input_phys_props) {
        return PhysicalProperty.ANY;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty[])
     */
    public Cost FindLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        return new Cost(100);
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#InputReqdProp(PhysicalProperty, LogicalProperty, int)
     */
    public PhysicalProperty[] InputReqdProp(
        PhysicalProperty PhysProp,
        LogicalProperty InputLogProp,
        int InputNo) {
        return new PhysicalProperty[] {};
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        return this;
    }

    /**
     * @see niagara.optimizer.colombia.Op#getName()
     */
    public String getName() {
        return "ConsolidateMe";
    }

    /**
     * @see niagara.optimizer.colombia.Op#getArity()
     */
    public int getArity() {
        return arity;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        return (other != null && other instanceof ConsolidateMe && arity == ((ConsolidateMe) other).arity);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return arity;
    }
}
