/* $Id: O_EXPR.java,v 1.7 2003/09/12 23:52:50 vpapad Exp $
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

import java.util.Arrays;
import java.util.Comparator;

/*
   ============================================================
   O_EXPR - Task to Optimize a multi-expression
   ============================================================ 
   This task is needed only if we implement O_GROUP in the original way.
   This task fires all rules for the expression, in order of promise.
   when it is used for exploring, fire only transformation rules to prepare for a transform
*/
public class O_EXPR extends Task {

    private MExpr MExpr; //Which expression to optimize
    private boolean explore;
    // if this task is for exploring  Should not happen - see E_GROUP
    private boolean Last; // if this task is the last task for the group
    Cost EpsBound;
    // if global eps pruning is on, this is the eps bound of this task
    // else it is zero

    // XXX vpapad: was destructor
    public void delete() {
        if (Last) {
            Group Group = MExpr.getGroup();
            if (!explore) {
                Context localContext = context;
                //What prop is required of
                PhysicalProperty LocalReqdProp = localContext.getPhysProp();
                Winner Winner = Group.getWinner(LocalReqdProp);

                // mark the winner as done
                if (Winner != null)
                    assert(!Winner.getDone());

                Winner.setDone();
                // this's still the last applied rule in the group, 
                // so mark the group with completed optimizing
                Group.setOptimized(true);
            } else
                Group.setExplored(true);
        }
    }

    public O_EXPR(
        SSP ssp,
        MExpr mexpr,
        boolean isExplore,
        Context context,
        boolean last,
    // default is false
    Cost bound // default is null
    ) {
        super(ssp, context);
        MExpr = mexpr;
        explore = isExplore;
        Last = last;
        EpsBound = bound;
    }; //O_EXPR

    public O_EXPR(
        SSP ssp,
        MExpr mexpr,
        boolean explore,
        Context context,
        boolean last) {
        this(ssp, mexpr, explore, context, last, null);
    }

    public void perform() {
        //        PTRACE2(
        //            "O_EXPR performing, %s mexpr: %s ",
        //            explore ? "exploring" : "optimizing",
        //            (const char *) MExpr.Dump());
            //        PTRACE2 ("Context ID: %d , %s", ContextID, 
            //                         (const char *) Context::vc[ContextID].Dump());
        //        PTRACE ("Last flag is %d", Last);

        if (explore)
            //explore is only for logical expression
            assert MExpr.getOp().is_logical();

        if (MExpr.getOp().is_item()) {
            //PTRACE("%s", "expression is an item_op");
            //push the O_INPUT for this item_expr
            //PTRACE0("pushing O_INPUTS " + MExpr.Dump());
            if (ssp.GlobepsPruning) {
                Cost eps_bound = new Cost(EpsBound);
                ssp.addTask(new O_INPUTS(MExpr, context, true, eps_bound));
            } else
                ssp.addTask(new O_INPUTS(MExpr, context, true));
            delete();
            return;
        }

        // identify valid and promising rules
        Move[] Move = new Move[ssp.getRulesetSize()];
        int moves = 0; // # of moves already collected
        for (int RuleNo = 0; RuleNo < ssp.getRulesetSize(); RuleNo++) {
            Rule Rule = ssp.getRule(RuleNo);

            // XXX vpapad: in previous versions, rules were set to null
            // now SSP is supposed to only return valid rules

            assert Rule != null;

            if (!Rule.canFire(MExpr)) {
                ssp.getTracer().ruleMasked(Rule, MExpr);
                continue; // rule has already fired
            }

            if (explore && Rule.getSubstitute().getOp().is_physical()) {
                //            PTRACE(
                //                "Rejected rule %d, only fire logical rules ",
                //                Rule.get_index());
                continue; // only fire transformation rule when exploring
            }

            double promise;

            
            // insert a valid and promising move into the array
            if (Rule.top_match(MExpr.getOp()) && (promise = Rule.promise(MExpr.getOp(), context)) > 0) {
                Move[moves] = new Move(promise, Rule);
                moves++;
            }
        }

        //    PTRACE("%d promising moves", moves);

        /* XXX vpapad: From Chris Dillard            
        Vassilis,
                                                                                
        I forgot to mention in my previous e-mail:  I made a small change that,
        surprisingly, improved performance quite a bit.
                                                                                
        In the old O_EXPR, you loop through every rule in the RuleSet, calling
        "top_match".  When the number of rules becomes large (as it is in my
        case), this is inefficient.  So my RuleSet builds a HashMap whose keys
        are Classes and whose values are ArrayLists of rules that could apply to
        that Op class (i.e. the root of the pattern is either that class or
        LeafOp).
                                                                                
        So O_EXPR's main loop first gets the appropriate ArrayList then only
        loops over those potential rules.
        */

        /* XXX vpapad: Reported by Chris Dillard
         * There's a bug in the interaction between
         * E_GROUP and O_GROUP. When a group is just
         * explored, logical multiexpressions may be added
         * to it, but no physical multiexpressions are
         * generated. When this group is subsequently
         * optimized, the logical multiexpressions 
         * generated by E_GROUP will not be generated
         * again (they'd be duplicates), and thus no
         * implementation rules will ever be applied to
         * them.
         * 
         * One workaround is to make an O_EXPR push
         * O_EXPRs on the following multiexpressions.
         * This may be the same issue that Quan talks
         * about below.
         */
         
        // HACK: do not know why 
        // When no move for the first mexpr, the next one will not be optimized,
        // SO add this
        if ((moves == 0) && (MExpr.getNextMExpr() != null)) {
            //PTRACE0("push O_EXPR " + MExpr.GetNextMExpr().Dump());
            ssp.addTask(
                new O_EXPR(
                    ssp,
                    MExpr.getNextMExpr(),
                    explore,
                    context,
                    true));
            // XXX vpapad: Should there be a call to delete() here?
            return;
        }


        // order the valid and promising moves by their promise, descending
        Arrays.sort(Move, 0, moves, new Comparator() {
            public int compare(Object o1, Object o2) {
                Move m1 = (Move) o1;
                Move m2 = (Move) o2;
                return -m1.compareTo(m2);
            }
        });
        // optimize the rest rules in order of promise
        while (--moves >= 0) {
            boolean Flag = false;
            if (Last)
                // this's the last task in the group,pass it to the new task
                {
                Last = false;
                // turn off this, since it's no longer the last task
                Flag = true;
            }

            // push future tasks in reverse order (due to LIFO stack)
            Rule Rule = Move[moves].rule;
            //PTRACE("pushing rule `%s'", (const char *) Rule.GetName());

            // apply the rule
            if (ssp.GlobepsPruning) {
                Cost eps_bound = new Cost(EpsBound);
                ssp.addTask(
                    new ApplyRule(
                        ssp,
                        Rule,
                        MExpr,
                        explore,
                        context,
                        Flag,
                        eps_bound));
            } else
                ssp.addTask(
                    new ApplyRule(ssp, Rule, MExpr, explore, context, Flag));

            // for enforcer and expansion rules, don't explore patterns
            Expr pattern = Rule.getPattern();
            if (pattern.getOp().is_leaf())
                continue;

            // earlier tasks: explore all inputs to match the pattern    
            for (int input_no = pattern.getArity(); --input_no >= 0;) {
                // only explore the input with arity > 0
                if (pattern.getInput(input_no).getArity() > 0) {
                    // If not yet explored, schedule a task with new context
                    Group g = MExpr.getInput(input_no);
                    if (g.needsExploring()) {
                        //E_GROUP can not be the last task for the group
                        if (ssp.GlobepsPruning) {
                            Cost eps_bound = new Cost(EpsBound);
                            ssp.addTask(
                                new E_GROUP(
                                    ssp,
                                    g,
                                    context,
                                    false,
                                    eps_bound));
                        } else
                            ssp.addTask(new E_GROUP(ssp, g, context));
                    }
                }
            } // earlier tasks: explore all inputs to match the pattern

        } // optimize in order of promise
        delete();
    } //perform

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "Optimizing " + MExpr;
    }

}

/** 
A Move is a pair of rule and promise, used to sort rules according to their promise
*/
class Move implements Comparable {
    // An uninteresting move
    public static Move NO_PROMISE = new Move(0, null);

    public double promise;
    Rule rule;

    public Move(double promise, Rule rule) {
        this.promise = promise;
        this.rule = rule;
    }

    public int compareTo(Object o) {
        if (!(o instanceof Move))
            throw new ClassCastException("Expected Move, got: " + o.getClass());
        Move other = (Move) o;
        if (promise > other.promise)
            return 1;
        if (promise == other.promise)
            return 0;
        return -1;
    }
}
