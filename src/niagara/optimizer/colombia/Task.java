/* $Id: Task.java,v 1.3 2003/02/25 06:19:08 vpapad Exp $
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
A task is an activity within the search process.  The original task
is to optimize the entire query.  Tasks create and schedule each
other; when no pending tasks remain, optimization terminates.

  In Cascades and Columbia, tasks store winners in memos; they do not
  actually produce a best plan.  After the optimization terminates,
  SSP::CopyOut() is called to print the best plan.
  
	Task is an abstract class.  Its subclasses are specific tasks.
	  
        Tasks must destroy themselves when done!
*/
abstract public class Task {
    Task next; // Used by class PTASK

    protected int ContextID; // Index to Context::vc, the shared set of contexts
    
    protected SSP ssp;
    
    public abstract void perform();
    
    public void delete() {}

    public Task(SSP ssp, int ContextID) {
        this.ssp = ssp;
        this.ContextID = ContextID;
    }
    
    public abstract String toString();
}
