/* $Id: Winner.java,v 1.3 2003/02/25 06:19:08 vpapad Exp $
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
  Winner

  The key idea of dynamic programming/memoization is to save the
  results of searches for future use.  A Winner is such a result.  In
  general a Winner contains the MExpr which won a search plus the
  context (Context) used in the search.  Done = False, in a winner, means
  this winner is under construction; its search is not complete.
  
  Each group has a set of winners derived from previous searches of
  that group.  This set of winners is called a memo in the classic
  literature; here we call it a winner's circle (cf. the Group class).
  
  A winner can represent these cases, if Done is true:
  (1) If MPlan is not a null pointer:
  MPlan is the cheapest possible plan in this group with PhysProp.  
  *MPlan has cost *Cost.  This derives from a successful search.
  (2) If MPlan is a null pointer, and Cost is not null:
  All possible plans in this group with PhysProp cost more than *Cost.
  This derives from a search which fails because of cost.
  (3) If MPlan is a null pointer, and Cost is null:
  There can be no plan in this group with PhysProp
  (Should never happen if we have enforcers)
  
  While the physical mexpressions of a group are being costed
  (i.e. Done=false), the cheapest yet found is stored in a winner.
*/
public class Winner {

    private MExpr  mPlan;
    // physProp and cost typically represent the context of the search
    // which generated this winner.
    private PhysicalProperty  physProp; 
    private Cost cost;   
    
    private boolean done; //Is this a real winner?
    
    public Winner(MExpr mexpr, PhysicalProperty physProp, Cost cost, boolean done) {
        this.cost = cost;
        if (mexpr == null)
            mPlan = null;
        else
            mPlan = new MExpr(mexpr);
        this.physProp = physProp;
        this.done = done;
    }

    public MExpr getMPlan() { return mPlan; }
    public PhysicalProperty getPhysProp() { return physProp; }
    public Cost getCost() { return cost; }
    public boolean getDone() { return done; }
    public void setDone() { done = true; }
}

