package niagara.optimizer.colombia;

import java.util.ArrayList;

/*
  ============================================================
  MULTIWINNERS of a search
  ============================================================
  A M_WINNER (multiwinner) data structure will, when the group is optimized, contain a 
  winner (best plan) for each interesting and relevant (i.e., in the schema) property of 
  the group.  The M_WINNER elements are initialized with an infinite bound to indicate
  that no qualifying plan has yet been found.
  
  The idea is then to optimize a group not only for the motivating context but also for 
  all the contexts stored in a M_WINNER for that group. In this way, we do not have to 
  revisit that group, thus saving CPU time and memory.
*/

public class M_WINNER { 
    M_WINNER(int S)    {

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

