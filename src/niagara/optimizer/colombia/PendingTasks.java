/* $Id: PendingTasks.java,v 1.3 2003/02/25 06:19:08 vpapad Exp $
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
PendingTasks is a collection of undone tasks is currently stored as a stack.
Other structures are certainly appropriate, but in any case dependencies
must be stored.  For example, a directed graph could be used to
parallelize optimization.
*/
public class PendingTasks {

    private Task first; // anchor of PendingTasks stack

    boolean empty() {
        return (first == null);
    } 

    void push(Task task) {
        task.next = first;
        first = task; //Push Task
    }

    Task pop() {
        if (empty())
            return null;

        Task task = first;
        first = task.next;
        return task;
    }
    
    public void clear() {
        first = null;
    }
}
