package niagara.optimizer.colombia;

/**
 * A task is an activity within the search process. The original task is to
 * optimize the entire query. Tasks create and schedule each other; when no
 * pending tasks remain, optimization terminates.
 * 
 * In Cascades and Columbia, tasks store winners in memos; they do not actually
 * produce a best plan. After the optimization terminates, SSP::CopyOut() is
 * called to print the best plan.
 * 
 * Task is an abstract class. Its subclasses are specific tasks.
 * 
 * Tasks must destroy themselves when done!
 */
abstract public class Task {
	Task next; // Used by class PTASK

	protected Context context;

	protected SSP ssp;

	public abstract void perform();

	public void delete() {
	}

	public Task(SSP ssp, Context context) {
		this.ssp = ssp;
		this.context = context;
	}

	public abstract String toString();
}
