package niagara.optimizer.colombia;

import java.util.BitSet;

/**
   MULTI_EXPRESSIONS

   M_EXPR is a compact form of EXPR which utilizes sharing.  Inputs are
   GROUPs instead of EXPRs, so the M_EXPR embodies several EXPRs.
   All searching is done with M_EXPRs, so each contains lots of state.
*/
public class M_EXPR {
    private static final int NEW_GRPID = -1;

// XXX vpapad private:
    private M_EXPR hashPtr;      // list within hash bucket
        
    private BitSet ruleMask; //If 1, do not fire rule with that index
    
    private int counter; // how many winners point to this MEXPR
    private OP         op;    //Operator
    private int[]     inputs;
    private int      grpID;         //I reside in this group
    
    // link to the next mexpr in the same group 
    private M_EXPR nextMExpr;
    
/*
//This struct will be replaced with more efficient and flexible storage
//Histor of which rules have been fired on this M_EXPR
        struct RuleHist {
        } RuleHist;
*/
    // XXX vpapad public:
        
    /*M_EXPR(OP * Op, 
                GRP_ID First = null, GRP_ID Second = null, 
                GRP_ID Third = null, GRP_ID Fourth = null);
    */
        
    //M_EXPR(OP * Op, GRP_ID* inputs); //Used by CopyIn
    

//     M_EXPR(EXPR * Expr, GRP_ID GrpID);

//     M_EXPR(M_EXPR& other); // used by NewWinner()
    
     int getCounter() { return counter; }
     void incCounter () { counter++; }
     void decCounter() { if (counter != 0) counter--; }
     OP getOp() {return op; } ;
     int getInput(int i) { return inputs[i]; }
     void setInput(int i, int grpId) { inputs[i] = grpId; }
     int getGrpID() { return grpID; } ;
     int getArity() { return op.getArity(); }
    
     M_EXPR  getNextHash() { return hashPtr; }
     void setNextHash(M_EXPR mexpr) { hashPtr = mexpr; }
    
     void setNextMExpr(M_EXPR mExpr) { nextMExpr = mExpr;}
     M_EXPR getNextMExpr() { return nextMExpr;}
    
    //We just fired this rule, so update dont_fire bit vector
     private void fire_rule(int rule_no) { 
	 ruleMask.set(rule_no);
     }

    //Can I fire this rule?
     private boolean can_fire(int rule_no) { 
	 return !ruleMask.get(rule_no);
    }

    // Transform an EXPR into an M_EXPR.  
    // May involve creating new Groups.
    public M_EXPR(EXPR expr, int grpid) {        
// 	grpID = (grpid==NEW_GRPID) ? Ssp->GetNewGrpID() : grpid;
// 	op = expr->getOp()->copy();
//         int gid;
//         EXPR input;
//         counter = 0;
// 	ruleMask = new BitSet(ssp.getRulesetSize());

//         if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR]->New(); 

//         // copy in the sub-expression
//         int arity = GetArity();
//         if(arity) 
//         {
//           Inputs = new GRP_ID [arity];
//           for(int i=0; i<arity; i++)
//           {
//                 input = Expr->GetInput(i);

//                 if(input -> GetOp() -> is_leaf())       // deal with LEAF_OP, sharing the existed group
//                         GID = ((LEAF_OP *)input->GetOp()) -> GetGroup();
//                 else
//                 {               
//                         // create a new sub group
//                         GID = NEW_GRPID;
//                         M_EXPR * MExpr = Ssp->CopyIn(input ,  GID);
//                 }

//                 Inputs[i] = GID ;
//           }  
//         }  // if(arity)
}

// copy constructor
// for now, only use for copy into winner circle, 
// so only consider physical mexpr, omitting some data members
// M_EXPR(M_EXPR & other)
//         :       Op(other.GetOp()->Copy()), GrpID(other.GetGrpID())
// {
//         assert(Op->is_physical() || Op->is_item());

//         if (!ForGlobalEpsPruning) ClassStat[C_M_EXPR]->New(); 

//         int arity = Op->GetArity() ;
//         if (arity)
//         {
//                 Inputs = new GRP_ID [ arity ];
//                 while(--arity >= 0)
//                         Inputs[arity] = other.GetInput(arity);
//         }

// }


// ub4 hash() {
//     ub4 hashval = Op->hash();

//         //to check the equality of the inputs
//     for(int input_no = Op->GetArity();  -- input_no >= 0;) 
//         hashval = lookup2(GetInput(input_no), hashval);

//         return (hashval % (HtblSize -1)) ;
// }

// CString LightDump() {
//         CString os;
//         CString temp;
        
//         os = (*Op).Dump();
        
//         int Size = GetArity();
//         for(int i=0;i<Size;i++)
//         {
//                 temp.Format("%s%d", " , ", Inputs[i]);
//                 os += temp;
//         }
                
//         return os;
// }
// CString Dump()
// {
//         CString os;
//         CString temp;
        
//         os = (*Op).Dump();
        
//         int Size = GetArity();
//         for(int i=0;i<Size;i++)
//         {
//                 temp.Format("%s%d", " , ", Inputs[i]);
//                 os += temp;
//         }
        
//         temp += "[" + FindLocalCost()->Dump() + "]; ";
//         os += temp;
        
//         return os;
// }
// Assisting Function -- will call PHYS_OP::FindLocalCost()
// COST FindLocalCost() {
//         LOG_PROP LocalLogProp = Ssp->GetGroup(GetGrpID())->GetLogProp();

//         LOG_PROP **LogProps = new LOG_PROP* [GetArity()];

//         for(int i=0;i<GetArity();i++)
//         {
//                 LogProps[i]=Ssp->GetGroup(GetInput(i))->GetLogProp();
//         };
                
//         return ((PHYS_OP*)Op)->FindLocalCost(LocalLogProp, LogProps);

// void clear_rule_mask() { clear(RuleMask); };
// void set_rule_mask(BIT_VECTOR v) { RuleMask=v;};
// void add_rule_mask(BIT_VECTOR v) { add_bit_vector(RuleMask,v);};

}

