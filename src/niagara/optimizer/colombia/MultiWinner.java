/* $Id: MultiWinner.java,v 1.2 2003/02/25 06:19:08 vpapad Exp $
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

/*
  ============================================================
  MULTIWINNERS of a search
  ============================================================
  A MultiWinner (multiwinner) data structure will, when the group is optimized, contain a 
  winner (best plan) for each interesting and relevant (i.e., in the schema) property of 
  the group.  The MultiWinner elements are initialized with an infinite bound to indicate
  that no qualifying plan has yet been found.
  
  The idea is then to optimize a group not only for the motivating context but also for 
  all the contexts stored in a MultiWinner for that group. In this way, we do not have to 
  revisit that group, thus saving CPU time and memory.
*/

public class MultiWinner { 
    MultiWinner(int S)    {

		wide = S;
		PhysProp = new PhysicalProperty[S];
		Bound = new Cost[S];
		BPlan = new MExpr[S];
		
		// set the first physical property as "any" for all groups
		PhysProp[0] = new PhysicalProperty(new Order(Order.Kind.ANY));
		
		// set the cost to INF and plan to null initially for all groups
		for (int i=0; i<S; i++)
		{
			Bound[i] = new Cost(-1);
			BPlan[i] = null;
		}
    }
    

  private    int wide;	// size of each element
    private MExpr[] BPlan;
    private PhysicalProperty[] PhysProp;
    Cost[] Bound;
    
  public   int GetWide() { return wide; }
    
    // Return the requested MEXPR indexed by an integer 
     MExpr  getBPlan(int i) { return(BPlan[i]); } ;
    
    // Return the MEXPR indexed by the physical property
     MExpr  getBPlan(PhysicalProperty PhysProp) {
        for (int i=0; i<wide; i++) {
            if (GetPhysProp(i).equals(PhysProp)) {
                return (BPlan[i]);
            }
        }
        return null; 
    }
	
     void SetPhysProp(int i, PhysicalProperty Prop) {
        PhysProp[i] = Prop;
    }

    // Return the requested physical property from multiwinner
     PhysicalProperty GetPhysProp(int i) { return (PhysProp[i]); }
    
    //Return the upper bound of the required physical property
     Cost getUpperBd(PhysicalProperty physProp) { 
        for (int i=0; i<wide; i++) {
            if (GetPhysProp(i).equals(PhysProp)) {
                return (Bound[i]);
            }
        }
        return (new Cost(-1)); 
    }

    //  Update bounds, when we get new bound for the context.
     void	SetUpperBound (Cost NewUB, PhysicalProperty Prop) {
        for (int i=0; i<wide; i++) {
            if (GetPhysProp(i).equals(Prop)) {
                Bound[i] = NewUB ; 
            }
        }
    }

    // Update the MEXPR
     void SetBPlan (MExpr Winner, int i) {
	BPlan[i] = Winner; 
    }
}

