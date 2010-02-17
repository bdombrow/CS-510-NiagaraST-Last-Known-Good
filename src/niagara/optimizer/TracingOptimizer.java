package niagara.optimizer;

import niagara.optimizer.colombia.Group;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.Rule;
import niagara.optimizer.colombia.Task;
import niagara.optimizer.colombia.Winner;

public class TracingOptimizer extends Optimizer {
	// Implementation of the Tracer interface
	int eventNumber;

	private void nextEvent() {
		// XXX here we should check for breakpoints and such
		eventNumber++;
	}

	/** Simple debugging to stderr */
	private void debugEvent(String msg) {
		System.err.println(eventNumber + ":" + msg);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#addingTask(Task)
	 */
	public void addingTask(Task task) {
		nextEvent();
		debugEvent("New task added: " + task);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#afterNewGroup(Group)
	 */
	public void afterNewGroup(Group group) {
		nextEvent();
		debugEvent("New group created: " + group);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#beforeNewGroup(MExpr)
	 */
	public void beforeNewGroup(MExpr mexpr) {
		nextEvent();
		debugEvent("New group about to be created for: " + mexpr);
	}

	public void newMExpr(MExpr mexpr) {
		nextEvent();
		debugEvent("A new multiexpression " + mexpr + " was created");
		// int x = 10; // XXX vpapad
	}

	public void addedMExprToGroup(MExpr mexpr) {
		nextEvent();
		debugEvent("Multiexpression " + mexpr + " was added to "
				+ mexpr.getGroup());
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#performingTask(Task)
	 */
	public void performingTask(Task task) {
		nextEvent();
		debugEvent("Performing task: " + task);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#endingOptimization()
	 */
	public void endingOptimization() {
		nextEvent();
		debugEvent("Optimization ended");
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#startingOptimization()
	 */
	public void startingOptimization() {
		// Reset the event number
		eventNumber = 0;
		nextEvent();
		debugEvent("Beginning optimization");
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#ruleMasked(Rule, MExpr)
	 */
	public void ruleMasked(Rule rule, MExpr mexpr) {
		nextEvent();
		debugEvent("Rule " + rule + " was masked for " + mexpr);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#duplicateMExprFound(MExpr)
	 */
	public void duplicateMExprFound(MExpr mexpr) {
		nextEvent();
		debugEvent("A duplicate multiexpression was found: " + mexpr);
	}

	/**
	 * @see niagara.optimizer.colombia.Tracer#newWinner(Group, Winner)
	 */
	public void newWinner(Group g, Winner w) {
		nextEvent();
		debugEvent("A new winner was found for " + g + " with cost "
				+ w.getCost());
	}
}
