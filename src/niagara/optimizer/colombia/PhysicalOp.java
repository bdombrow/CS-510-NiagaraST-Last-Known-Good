/* $Id: PhysicalOp.java,v 1.5 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.colombia;

import java.util.ArrayList;

/**
 *   PHYSICAL OPERATOR - class PhysicalOp
*/
public abstract class PhysicalOp extends Op {
    /** Logical properties of this operator's group */
    protected LogicalProperty logProp;

    public PhysicalOp() {
    }

    public PhysicalOp(LogicalProperty logProp) {
        this.logProp = logProp;
    }

    PhysicalOp(PhysicalOp other) {
        logProp = other.getLogProp();
    }

    /** What physical properties will this operator guarantee, given
     * the physical properties of its inputs? */
    public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
        // Default implementation: we don't guarantee anything
        return PhysicalProperty.ANY;
    }
    
    public static final PhysicalProperty[] NO_INPUTS = {};

    private static final PhysicalProperty[] ANY_PROPERTY_WILL_DO = {};
    /** Local cost of the operator, including output but not input costs.  
     * Thus we compute output costs only once, and get input costs from (as part of) 
     * the input operators' cost */
    public abstract Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp);

    /*  Some algorithms and implementation rules require that
       their inputs be optimized for multiple physical property
       combinations, e.g., a merge-join with multiple equality clauses
       "R.a == S.a && R.b == S.b" could benefit from sort order on
       "a", "b", "a, b", and "b, a".  For now we optimize for only
       one ordering, but later we may need:
    	 int opt_combs() */

    //If we require the physical property Prop of this operator, what
    //property from input number InputNo will guarantee it?
    //Should never be called for arity 0 operators

    // XXX vpapad Original definition used a by-ref parameter called 
    // possible. The semantics was: 
    // possible = true && return null -> any input property will do
    // possible = true && return non-null -> we require a specific property
    // possible = false => impossible to satisfy this property
    // Now, we return an array of physical properties. 
    // The semantics now:
    // return value null -> impossible to satisfy
    // return value empty -> any property will do
    // return value non-empty -> any of the listed properties will do
    public PhysicalProperty[] inputReqdProp(
        PhysicalProperty PhysProp,
        LogicalProperty InputLogProp,
        int InputNo) {
            // Default definition: we can only satisfy the "ANY" property
            if (PhysProp.equals(PhysicalProperty.ANY))  
                return ANY_PROPERTY_WILL_DO;    
            return null;
        }

    public final boolean is_physical() {
        return true;
    }

    protected LogicalProperty getLogProp() {
        return logProp;
    }

    public void setLogProp(LogicalProperty logProp) {
        this.logProp = logProp;
    }
}
