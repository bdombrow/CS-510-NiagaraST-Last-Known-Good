package niagara.optimizer.colombia;

import java.util.ArrayList;
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
