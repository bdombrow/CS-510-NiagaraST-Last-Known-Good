/* $Id: PhysicalOp.java,v 1.10 2003/09/16 04:45:29 vpapad Exp $
   Colombia -- Java version of the Columbia Database Optimization Framework

   Copyright (c)    Dept. of Computer Science , Portland State
   University and Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS, THE DEPT. OF COMPUTER SCIENCE DEPT. OF PORTLAND STATE
   UNIVERSITY AND DEPT. OF COMPUTER SCIENCE & ENGINEERING AT OHSU ALLOW
   USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, AND THEY DISCLAIM ANY
   LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE
   USE OF THIS SOFTWARE.

   This software was developed with support of NSF grants IRI-9118360,
   IRI-9119446, IRI-9509955, IRI-9610013, IRI-9619977, IIS 0086002,
   and DARPA (ARPA order #8230, CECOM contract DAAB07-91-C-Q518).
*/

package niagara.optimizer.colombia;

/**
 *   PHYSICAL OPERATOR - class PhysicalOp
*/
public abstract class PhysicalOp extends Op {
    /** Logical properties of this operator's group */
    protected LogicalProperty logProp;

    /** does this operator need a sendImmediate output stream?
     *  This can be true because the operator itself requires
     *  a send Immed stream (i.e. timer thread) or can be true
     *  because it was set to be true due to this operators's
     *  placement in the query tree
     */
    protected boolean isSendImmediate;

    public PhysicalOp() {
    }

    public PhysicalOp(LogicalProperty logProp) {
        this.logProp = logProp;
    }

    PhysicalOp(PhysicalOp other) {
        logProp = other.getLogProp();
	isSendImmediate = other.isSendImmediate;
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

    /**
    If we require the physical property Prop of this operator, what
    property from input number InputNo will guarantee it?
    Should never be called for arity 0 operators
    
    XXX vpapad Original definition used a by-ref parameter called 
    possible. The semantics was: 
    possible = true && return null -> any input property will do
    possible = true && return non-null -> we require a specific property
    possible = false => impossible to satisfy this property
    Now, we return an array of physical properties. 
    The semantics now:
    return value null -> impossible to satisfy
    return value empty -> any property will do
    return value non-empty -> any of the listed properties will do */
    public PhysicalProperty[] inputReqdProp(
        PhysicalProperty requiredOutputProp,
        LogicalProperty inputLogProp,
        int inputNo) {
            // Default definition: we can only satisfy the "ANY" property
            if (requiredOutputProp.equals(PhysicalProperty.ANY))  
                return ANY_PROPERTY_WILL_DO;    
            return null;
        }

    public final boolean isPhysical() {
        return true;
    }

    protected LogicalProperty getLogProp() {
        return logProp;
    }

    public void setLogProp(LogicalProperty logProp) {
        this.logProp = logProp;
    }

    public boolean isSendImmediate() {
	return isSendImmediate;
    }

    public void setSendImmediate() {
	isSendImmediate = true;
    }
}
