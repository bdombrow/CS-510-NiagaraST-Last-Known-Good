/* $Id: Context.java,v 1.4 2003/02/25 06:19:07 vpapad Exp $ 
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
 *   CONTEXTs/CONSTRAINTS on a search
 */
public class Context {
    // Each search for the cheapest solution to a problem or
    // subproblem is done relative to some conditions, also called
    // constraints.  In our context, a condition consists of
    // required (not excluded) properties (e.g. must the solution be
    // sorted) and an upper bound (e.g. must the solution cost less than 5).

    // We are not using a lower bound.  It is not very effective.
    // Each search spawns multiple subtasks, e.g. one task to fire each rule for the search.
    // These subtasks all share the search's Context.  Sharing is done
    // not only to save space, but to share information about when
    // the search is done, what is the current upper bound, etc.

    private PhysicalProperty reqdPhys;
    private Cost upperBd;
 
    // Finish is true means the task is done.
    private boolean finished;

    public PhysicalProperty getPhysProp() {
        return reqdPhys;
    }
    
    public Cost getUpperBd() {
        return upperBd;
    }

    public void setPhysProp(PhysicalProperty rp) {
        reqdPhys = rp;
    }

    // set the flag if the context is done, means we completed the search,
    // may got a final winner, or found out optimal plan for this context not exist
    public void setFinished() {
        finished = true;
    }
    public boolean isFinished() {
        return finished;
    }

    //  Update bounds, when we get better ones.
    void setUpperBound(Cost newUB) {
        upperBd = newUB;
    }

    public Context(PhysicalProperty reqdPhys, Cost upperBd, boolean finished) {
        this.reqdPhys = reqdPhys;
        this.upperBd = upperBd;
        this.finished = finished;
    }

    public String toString() {
        return "Property: " + reqdPhys + " UpperBound: " + upperBd;
    }
}
