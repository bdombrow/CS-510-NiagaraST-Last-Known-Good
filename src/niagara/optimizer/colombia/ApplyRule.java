package niagara.optimizer.colombia;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
  ============================================================
  ApplyRule - Task to Apply a rule to a Multi-Expression
  ============================================================ 
  
*/
public class ApplyRule extends Task {
    private Rule rule; // rule to apply
    private MExpr mexpr; // root of expr. before rule
    private boolean explore; // if this task is for exploring
    private boolean last; // if this task is the last task for the group
    Cost epsBound;
    // if global eps pruning is on, this is the eps bound for eps pruning
    // else it is zero

    public ApplyRule(
        SSP ssp,
        Rule rule,
        MExpr mexpr,
        boolean explore,
        int ContextID,
        boolean last,
        Cost bound) {
        super(ssp, ContextID);
        this.ssp = ssp;
        this.rule = rule;
        this.mexpr = mexpr;
        this.explore = explore;
        this.last = last;
        this.epsBound = bound;
    }

    public ApplyRule(
        SSP ssp,
        Rule rule,
        MExpr mexpr,
        boolean explore,
        int ContextID,
        boolean last) {
        this(ssp, rule, mexpr, explore, ContextID, last, null);
    }

    // XXX vpapad: was destructor
    public void delete() {
        if (last) {
            Group Group = mexpr.getGroup();
            if (!explore) {
                if (!ssp.IRPROP) {
                    Context LocalCont = ssp.getVc(ContextID);
                    //What prop is required of
                    PhysicalProperty LocalReqdProp = LocalCont.getPhysProp();
                    Winner Winner = Group.getWinner(LocalReqdProp);

                    if (Winner != null)
                        assert !Winner.getDone();

                    // mark the winner as done
                    Winner.setDone();
                }
                // this's still the last applied rule in the group, 
                // so mark the group with completed optimization or exploration
                Group.setOptimized(true);
            } else
                Group.setExplored(true);
        }
    }

    public void perform() {
        Context Context = ssp.getVc(ContextID);
        //        PTRACE2 ("ApplyRule performing, rule: %s expression: %s", 
        //            (const char *) rule.GetName(), 
        //                         (const char *) mexpr.Dump());
        //        PTRACE2 ("Context ID: %d , %s", ContextID, 
        //                         (const char *) Context::vc[ContextID].Dump());
        //        PTRACE ("Last flag is %d", Last);

        //if stop generating logical expression when epsilon prune is applied
        //if this context is done, stop
        if (!ssp.GEN_LOG) {
            //Check that this context is not done
            if (Context.isFinished()) {
                delete();
                return;
            }
        } else {
            //if not stop generating logical expression when epsilon prune is applied
            //if this context is done and the substitute is physical, if the substitute
            //is logical continue
            if (Context.isFinished() && rule.is_log_to_phys()) {
                delete();
                return;
            }
        }
        //Check again to see that the rule has not been fired since this was put on the stack
        if (!rule.canFire(mexpr)) {
            ssp.getTracer().ruleMasked(rule, mexpr);
            delete();
            return;
        }

        // main variables for the loop over all possible bindings
        Bindery bindery; // Expression bindery.
        //    Used to bind expr to rule's pattern

        //be sure to delete all these after they are used.
        Expr before; // see below
        Expr after; // see below

        MExpr NewMExpr; // version of "after" placed in MEMO

        //Guide to closely related variables

        //    pattern 
        //     ApplyRule has a rule member data.  pattern is member data
        //     of that rule.  It describes (as an Expr) existing expressions 
        //     to be bound,
        //  sustitute
        //     from the same rule, as with pattern.  Describes (as an Expr)
        //     the new expression.
        //  before
        //     the existing expression which is currently bound to the pattern
        //     by the bindery.
        //  after
        //     the new expression, in Expr form, corresponding to the substitute.
        //  NewMExpr
        //     the new expression, in MEXPR form, which has been included in the
        //     search space.

        // Loop over all Bindinges of expr to pattern of rule
        bindery = new Bindery(mexpr, rule.GetPattern(), ssp);
        int rule_matched = 0;
        int rule_fired = 0;
        if (!ssp.SORT_AFTERS) {
            while (bindery.advance()) {
                rule_matched++;
                // There must be a Binding since advance() returned non-null.
                // Extract the bound Expr from the bindery
                before = bindery.extract_expr();
                //            PTRACE0("new Binding is: " + before.Dump());
                //#ifdef _DEBUG
                //            Bindings[rule.get_index()]++;
                //#endif
                // check the rule's condition function
                Context Cont = ssp.getVc(ContextID);
                PhysicalProperty ReqdProp = Cont.getPhysProp();
                //What prop is required of

                if (!rule.condition(before, mexpr, ReqdProp)) {
                    //                PTRACE0("Binding FAILS condition function, expr: " + mexpr.Dump());
                    continue; // try to find another binding
                }
                // rule will actually be fired, rule_is_fired is useful later for setting before_mask 
                rule_fired++;

                //PTRACE0("Binding SATISFIES condition function.  Mexpr: " + mexpr.Dump());

                //#ifdef _DEBUG
                //            Conditions[rule.get_index()]++;
                //#endif
                // try to derive a new substitute expression
                after = rule.next_substitute(before, mexpr, ReqdProp);

                assert after != null;

                //            PTRAClE0("substitute expr is : " + after.Dump());

                // include substitute in MEMO, find duplicates, etc.
                int group_no = mexpr.getGrpID();

                if (ssp.NO_PHYS_IN_GROUP) {
                    // don't include physical mexprs into group
                    if (after.getOp().is_logical())
                        NewMExpr = ssp.copyIn(after, group_no);
                    else
                        NewMExpr = new MExpr(after, group_no, ssp);
                } else //include physical mexpr into group
                    NewMExpr = ssp.copyIn(after, group_no);

                // If substitute was already known 
                if (NewMExpr == null) {
                    // PTRACE0("duplicate substitute " + after.Dump());

                    after = null; // "after" no longer used

                    continue; // try to find another substitute
                }

                //            PTRACE0("New Mexpr is : " + NewMExpr.Dump());

                after = null; // "after" no longer used

                //Give this expression the rule's mask
                NewMExpr.setRuleMask(rule.getMask());

                //We need to handle this case for rules like project.null,
                //by merging groups
                assert mexpr.getGrpID() == NewMExpr.getGrpID();

                boolean Flag = false;
                if (last)
                    // this's the last applied rule in the group,pass it to the new task
                    {
                    last = false;
                    // turn off this, since it's no longer the last task
                    Flag = true;
                }

                // follow-on tasks
                if (explore) // optimizer is exploring, the new mexpr must be logical expr
                    {
                    assert NewMExpr.getOp().is_logical();
                    //                            PTRACE0("new task to explore new expression," 
                    //                "pushing O_EXPR exploring expr: " + NewMExpr.Dump());
                    if (ssp.GlobepsPruning) {
                        Cost eps_bound = new Cost(epsBound);
                        ssp.addTask(
                            new O_EXPR(
                                ssp,
                                NewMExpr,
                                true,
                                ContextID,
                                Flag,
                                eps_bound));
                    } else
                        ssp.addTask(
                            new O_EXPR(ssp, NewMExpr, true, ContextID, Flag));
                } // optimizer is exploring
                else { // optimizer is optimizing
                    // for a logical op, try further transformations
                    if (NewMExpr.getOp().is_logical()) {
                        //                    PTRACE0("new task to optimize new expression,pushing O_EXPR, expr: " + NewMExpr.Dump());
                        if (ssp.GlobepsPruning) {
                            Cost eps_bound = new Cost(epsBound);
                            ssp.addTask(
                                new O_EXPR(
                                    ssp,
                                    NewMExpr,
                                    false,
                                    ContextID,
                                    Flag,
                                    eps_bound));
                        } else
                            ssp.addTask(
                                new O_EXPR(
                                    ssp,
                                    NewMExpr,
                                    false,
                                    ContextID,
                                    Flag));
                    } // further transformations to optimize new expr
                    else {
                        // for a physical operator, optimize the inputs
                        /* must be done even if op_arg.arity == 0 in order to calculate costs */
                        assert NewMExpr.getOp().is_physical();
                        //                    PTRACE0("new task to optimize inputs,pushing O_INPUT, epxr: " + NewMExpr.Dump());
                        if (ssp.GlobepsPruning) {
                            Cost eps_bound = new Cost(epsBound);
                            ssp.addTask(
                                new O_INPUTS(
                                    NewMExpr,
                                    ContextID,
                                    Flag,
                                    eps_bound));
                        } else {
                            int contextNo = 0;
                            int j = 0;
                            if (ssp.IRPROP) {
                                if (last)
                                    last = false;

                                int GrpNo = NewMExpr.getGrpID();
                                // XXX vpapad: what is this?
                                if ((NewMExpr.getOp()).getName() == "QSORT")
                                    j = 1;
                                for (int i = j;
                                    i < ssp.getMc(GrpNo).GetWide();
                                    i++) {
                                    if (i != ContextID) {
                                        ssp.addTask(
                                            new O_INPUTS(
                                                NewMExpr,
                                                i,
                                                Flag,
                                                null,
                                                contextNo++));
                                    }
                                }
                                if (!(j == 1 && ContextID == 0))
                                    ssp.addTask(
                                        new O_INPUTS(
                                            NewMExpr,
                                            ContextID,
                                            Flag,
                                            null,
                                            contextNo++));
                            } else {
                                ssp.addTask(
                                    new O_INPUTS(
                                        NewMExpr,
                                        ContextID,
                                        Flag,
                                        null));
                            }
                        }

                    } // for a physical operator, optimize the inputs

                } // optimizer is optimizing

            } // try all possible bindings
        } else {
            // a temporary array just for holding the elements
            ArrayList AfterArray = new ArrayList();
            // get all the substitutions, put them in the array, sort the array
            // according to the estimanted cost, and push the most expensive task 
            // first, so that we can get lowest LB soon
            while (bindery.advance()) {
                rule_matched++;
                // There must be a Binding since advance() returned non-null.
                // Extract the bound Expr from the bindery
                before = bindery.extract_expr();
                // PTRACE ("new Binding is: %s", before.Dump());

                // check the rule's context function
                Context Cont = ssp.getVc(ContextID);
                PhysicalProperty ReqdProp = Cont.getPhysProp();
                //What prop is required of
                if (!rule.condition(before, mexpr, ReqdProp)) {
                    // PTRACE ("Binding FAILS condition function, expr: %s",mexpr.Dump());
                    continue; // try to find another binding
                }

                // rule will actually be fired, rule_is_fired is useful later for setting before_mask 
                rule_fired++;

                //PTRACE ("Binding SATISFIES condition function.  Mexpr: %s",mexpr.Dump());

                // try to derive a new substitute expression
                after = rule.next_substitute(before, mexpr, ReqdProp);

                assert(after != null);

                //PTRACE("substitute expr is : %s", after.Dump());

                // include substitute in MEMO, find duplicates, etc.
                int group_no = mexpr.getGrpID();

                if (ssp.NO_PHYS_IN_GROUP) {
                    // don't include physical mexprs into group
                    if (after.getOp().is_logical())
                        NewMExpr = ssp.copyIn(after, group_no);
                    else
                        NewMExpr = new MExpr(after, group_no, ssp);
                } else //include physcial mexpr into group
                    NewMExpr = ssp.copyIn(after, group_no);

                // If substitute was already known 
                if (NewMExpr == null) {
                    //PTRACE("duplicate substitute %s", after.Dump());

                    after = null; // "after" no longer used

                    continue; // try to find another substitute
                }

                //PTRACE("New Mexpr is : %s", NewMExpr.Dump());

                after = null; // "after" no longer used

                //Give this expression the rule's mask
                NewMExpr.setRuleMask(rule.getMask());

                AFTERS element = new AFTERS();
                element.m_expr = NewMExpr;
                //calculate the estimate cost
                Cost[] InputCost = null;
                Cost TotalCost = new Cost(0);
                Cost LocalCost;
                LogicalProperty[] InputLogProp = null;
                int arity = NewMExpr.getArity();
                if (arity > 0) {
                    InputCost = new Cost[arity];
                    InputLogProp = new LogicalProperty[arity];
                    int input;
                    for (input = 0; input < arity; input++) {
                        Group IG = NewMExpr.getInput(input);
                        InputCost[input] = IG.getLowerBd();
                        InputLogProp[input] = IG.getLogProp();
                    }
                }
                LogicalProperty LogProp = ssp.getGroup(group_no).getLogProp();

                // if it is physical operator, plus the local cost
                if (NewMExpr.getOp().is_physical())
                    LocalCost =
                        ((PhysicalOp) NewMExpr.getOp()).FindLocalCost(
                            ssp.getCatalog(),
                            InputLogProp);
                else
                    LocalCost = new Cost(0);
                TotalCost.finalCost(LocalCost, InputCost);
                element.cost = TotalCost;

                if (arity > 0) {
                    InputCost = null;
                    InputLogProp = null;
                }
                LocalCost = null;
                AfterArray.add(element);
            }
            int num_afters = AfterArray.size();
            // copy the array to static array
            AFTERS[] Afters = new AFTERS[num_afters];
            for (int array_index = 0;
                array_index < num_afters;
                array_index++) {
                Afters[array_index].m_expr =
                    ((AFTERS) AfterArray.get(array_index)).m_expr;
                Afters[array_index].cost =
                    ((AFTERS) AfterArray.get(array_index)).cost;
            }
            if (num_afters > 1) {
                // order tasks by descending cost
                Arrays.sort(Afters, 0, num_afters, new Comparator() {
                    public int compare(Object o1, Object o2) {
                        AFTERS m1 = (AFTERS) o1;
                        AFTERS m2 = (AFTERS) o2;
                        return -m1.compareTo(m2);
                    }
                });
            }
            // push tasks in the order of estimate cost, most expensive first
            while (--num_afters >= 0) {
                //Give this expression the rule's mask
                Afters[num_afters].m_expr.setRuleMask(rule.getMask());

                //We need to handle this case for rules like project.null,
                //by merging groups
                assert(
                    mexpr.getGrpID() == Afters[num_afters].m_expr.getGrpID());

                boolean Flag = false;
                if (last)
                    // this's the last applied rule in the group,pass it to the new task
                    {
                    last = false;
                    // turn off this, since it's no longer the last task
                    Flag = true;
                }

                // follow-on tasks
                if (explore) // optimizer is exploring, the new mexpr must be logical expr
                    {
                    assert(
                        ((AFTERS) AfterArray.get(num_afters))
                            .m_expr
                            .getOp()
                            .is_logical());
                    //PTRACE ("new task to explore new expression, \
                    //  pushing O_EXPR exploring expr: %s", Afters[num_afters].m_expr.Dump());
                    ssp.addTask(
                        new O_EXPR(
                            ssp,
                            Afters[num_afters].m_expr,
                            true,
                            ContextID,
                            Flag));

                } // optimizer is exploring
                else { // optimizer is optimizing
                    // for a logical op, try further transformations
                    if (Afters[num_afters].m_expr.getOp().is_logical()) {
                        //PTRACE("new task to optimize new expression,pushing O_EXPR, expr: %s", 
                        //     Afters[num_afters].m_expr.Dump());
                        ssp.addTask(
                            new O_EXPR(
                                ssp,
                                Afters[num_afters].m_expr,
                                false,
                                ContextID,
                                Flag));
                    } // further transformations to optimize new expr
                    else {
                        // for a physical operator, optimize the inputs
                        /* must be done even if op_arg.arity == 0 in order to calculate costs */
                        assert(Afters[num_afters].m_expr.getOp().is_physical());

                        //PTRACE("new task to optimize inputs,pushing O_INPUT, epxr: %s", 
                        //        Afters[num_afters].m_expr.Dump());
                        ssp.addTask(
                            new O_INPUTS(
                                Afters[num_afters].m_expr,
                                ContextID,
                                Flag));
                    } // for a physical operator, optimize the inputs

                } // optimizer is optimizing

                Afters[num_afters].cost = null;

            } // end while
            Afters = null;
        }

        bindery = null;

        //        PTRACE("The rules was matched for %d times", rule_matched);
        //        PTRACE("The rules was fired for %d times", rule_fired);

        // Add before-mask to mexpr's RuleMask, to enable that firing this rule 
        // will block certain other rules to be applied to this m-expr
        // An example is that non-duplicating unnesting rules will block duplicating
        // unnesting rules. 
        // This seems to be the only place you need to modify to deactivate or activate 
        // before_mask.    (Added 5/2000 Quan Wang)
        if (rule_fired > 0) {
            mexpr.addRuleMask(rule.getBeforeMask());
        }

        //Mark rule vector to show that this rule has fired
        mexpr.fire_rule(rule.get_index());

        delete();
    } // perform

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Apply rule: ");
        sb.append(rule);
        sb.append(" to mexpr ");
        sb.append(mexpr);

        return sb.toString();
    }

    /* Function to compare the cost of mexprs */
    int compare_afters(AFTERS x, AFTERS y) {
        int result = 0;
        if (x.cost.lessThan(y.cost))
            result = -1;
        else if (x.cost.greaterThan(y.cost))
            result = 1;
        else
            result = 0;

        return result;
    } // compare_afters

}

/**
 *  Pair of expr and cost value, used to sort expr according to their cost
 */
class AFTERS {
    public MExpr m_expr;
    public Cost cost;

    public int compareTo(Object o) {
        if (!(o instanceof AFTERS))
            throw new ClassCastException(
                "Expected AFTERS, got: " + o.getClass());
        AFTERS other = (AFTERS) o;
        if (cost.greaterThan(other.cost))
            return 1;
        if (cost.equals(other.cost))
            return 0;
        return -1;
    }
}
