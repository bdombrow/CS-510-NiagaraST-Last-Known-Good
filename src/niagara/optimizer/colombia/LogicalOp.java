/* $Id: LogicalOp.java,v 1.9 2003/09/16 04:45:29 vpapad Exp $ 
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

/** Logical operators */
public abstract class LogicalOp extends Op {
    /**
     * @return <code>true</code> if <code>this</code> and
     * <code>other</code> are the same operator, disregarding
     * arguments. OpMatch is used in preconditions for applying rules.
     * This should be moved to the Op class if we ever apply rules to
     * other than logical operators.
     */
    public boolean opMatch(Class other) {
        return getClass() == other;
    }

    /**
     * Determine the logical properties of this operator's output,
     * given the logical properties of its inputs
     */
    public abstract LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input);
        
    public boolean isLogical() {
        return true;
    }
}
