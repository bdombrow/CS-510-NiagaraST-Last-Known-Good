/* $Id: Tracer.java,v 1.4 2003/02/25 06:19:08 vpapad Exp $
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
 * A tracer object handles tracing events from the optimizer
 */
public interface Tracer {
    /** About to start optimization */
    void startingOptimization();
    
    /** Optimization just finished */
    void endingOptimization();
    
    /** About to create a new group with this multiexpression */
    void beforeNewGroup(MExpr mexpr);
    
    /** A new group was created */
    void afterNewGroup(Group group);

    /** A new multiexpression was created */
    void newMExpr(MExpr mexpr);

    /** A new winner was found for a group */
    void newWinner(Group g, Winner w);
        
    /** A multiexpression was added to a group */
    void addedMExprToGroup(MExpr mexpr);
    
    /** A new task is scheduled */
    void addingTask(Task task);
    
    /** A task is about to be performed*/
    void performingTask(Task task);
    
    /** Application of a rule was masked for a multiexpression */
    void ruleMasked(Rule rule, MExpr mexpr);
    
    /** A duplicate multiexpression was found */
    void duplicateMExprFound(MExpr mexpr);
}
