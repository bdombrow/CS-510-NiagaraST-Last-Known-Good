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
