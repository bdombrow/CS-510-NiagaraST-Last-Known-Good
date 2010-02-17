package niagara.query_engine;

import java.util.HashMap;

import niagara.connection_server.NiagraServer;
import niagara.data_manager.DataManager;
import niagara.ndom.DOMFactory;
import niagara.physical.PhysicalHead;
import niagara.physical.PhysicalOperator;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;
import niagara.utils.SourceTupleStream;

import org.w3c.dom.Document;

/**
 * The class <code>ExecutionScheduler</code> schedules optimized queries
 * (represented by the optimized logical operator tree) for execution.
 * 
 */
@SuppressWarnings("unchecked")
public class ExecutionScheduler {
	// The server we're running in
	// private NiagraServer server;

	// This is the data manager that the physical operators interact with
	protected DataManager dataManager;

	// This is the operator queue in which all operators scheduled for
	// execution are to be put
	private PhysicalOperatorQueue opQueue;

	// This specifies the responsiveness of operators
	private static Integer responsiveness = new Integer(100);

	// private boolean debug = false;

	/**
	 * This is the constructor for the ExecutionScheduler that initializes it
	 * with the operator queue in which it is to put operators scheduled for
	 * execution.
	 * 
	 * @param server
	 *            The Niagara server we're running in
	 * 
	 * @param dataManager
	 *            The data manager associated with the execution scheduler to be
	 *            contacted as necessary
	 * @param opQueue
	 *            The queue in which operators scheduled for execution are to be
	 *            put
	 */

	public ExecutionScheduler(NiagraServer server, DataManager dataManager,
			PhysicalOperatorQueue opQueue) {

		// this.server = server;
		this.opQueue = opQueue;
		this.dataManager = dataManager;
	}

	/**
	 * This is the function that schedules all the operators in the optimized
	 * logical operator tree for execution
	 * 
	 * @param logicalOpTree
	 *            The optimized logical operator tree
	 * @param queryInfo
	 *            Information about the query that is optimized
	 * 
	 * @param outputStream
	 *            The stream that returns result to user
	 */

	public synchronized void executeOperators(SchedulablePlan optimizedTree,
			QueryInfo queryInfo) throws ShutdownException {

		// Very first - set up the sendImmediate flags for
		// the streams
		setStreamFlags(optimizedTree);

		// First create a Physical Head Operator to handle this query
		// in the system, only need to do this when top node
		// is SourceOp and can't function as head

		// where the top node of optimizedTree should put its output
		PageStream opTreeOutput;
		if (optimizedTree.isSource()) {
			SinkTupleStream[] outputStreams = new SinkTupleStream[1];
			outputStreams[0] = new SinkTupleStream(queryInfo
					.getOutputPageStream());

			opTreeOutput = new PageStream(optimizedTree.getName()
					+ "-to-PhysicalHead");
			SourceTupleStream[] inputStreams = new SourceTupleStream[1];
			inputStreams[0] = new SourceTupleStream(opTreeOutput);

			if (optimizedTree.isSendImmediate()) {
				outputStreams[0].setSendImmediate();
			}

			PhysicalHead headOperator = new PhysicalHead(queryInfo,
					inputStreams, outputStreams, responsiveness);

			// Put this operator in the execution queue
			opQueue.putOperator(headOperator);
		} else {
			opTreeOutput = queryInfo.getOutputPageStream();

			// make the top operator function as a head op
			optimizedTree.setIsHead();
		}

		// Traverse the optimized tree and schedule the operators for
		// execution
		scheduleForExecution(optimizedTree, opTreeOutput, new HashMap(),
				DOMFactory.newDocument(), queryInfo);
	}

	public PageStream scheduleSubPlan(SchedulablePlan rootNode)
			throws ShutdownException {
		assert rootNode.isSchedulable();
		PageStream results = new PageStream("SubPlan");
		scheduleForExecution(rootNode, results, new HashMap(), DOMFactory
				.newDocument(), null);
		return results;
	}

	/**
	 * This function schedules the DAG rooted at "rootLogicalNode" for execution
	 * 
	 * @param node
	 *            The root of the the logical tree to be scheduled for execution
	 * @param outputStream
	 *            The stream to which the output is to be sent
	 * @param nodesScheduled
	 *            A hashtable containing all the logical plan nodes that are
	 *            already scheduled, since the plan is not necessarily a tree.
	 * @param doc
	 *            The DOM document that will own any newly created XML nodes
	 */
	private void scheduleForExecution(SchedulablePlan node,
			PageStream outputStream, HashMap nodesScheduled, Document doc,
			QueryInfo queryInfo) throws ShutdownException {
		if (nodesScheduled.containsKey(node)) {
			// This operator was already scheduled
			// Just add outputStream to its output streams
			// XXX vpapad: Here we assume that source operators cannot
			// have multiple output streams
			PhysicalOperator physOp = (PhysicalOperator) nodesScheduled
					.get(node);
			physOp.addSinkStream(new SinkTupleStream(outputStream));
			if (physOp.isReady())
				opQueue.putOperator(physOp);
			return;
		}

		// Handle the scan operators differently - these are operators
		// that can only appear at the very bottom of a query tree and
		// provide input for the query
		if (node.isSource()) {
			// all source ops use SinkTupleStreams for output and these
			// streams must reflect GET_PARTIALS
			SinkTupleStream sinkStream = new SinkTupleStream(outputStream, true);
			node.processSource(sinkStream, dataManager, opQueue);
			// XXX vpapad: we no longer have an operator to put here
			nodesScheduled.put(node, null);
		} else {
			// regular operator node - create the output streams array
			SinkTupleStream[] outputStreams = new SinkTupleStream[node
					.getNumberOfOutputs()];
			outputStreams[0] = new SinkTupleStream(outputStream);

			// Recurse over all children and create input streams array
			int numInputs = node.getArity();

			SourceTupleStream[] inputStreams = new SourceTupleStream[numInputs];

			for (int child = 0; child < numInputs; ++child) {
				// Create a new input stream
				SchedulablePlan childPlan = node.getInput(child);

				PageStream inputPageStream = new PageStream(childPlan.getName()
						+ "-to-" + node.getName());
				inputStreams[child] = new SourceTupleStream(inputPageStream);

				// Recurse on child
				scheduleForExecution(childPlan, inputPageStream,
						nodesScheduled, doc, null);
			}

			// Get the physical operator -
			PhysicalOperator physicalOperator = node.getPhysicalOperator();
			physicalOperator.plugInStreams(inputStreams, outputStreams,
					dataManager, responsiveness);

			if (node.isHead()) {
				physicalOperator.setAsHead(queryInfo);
			}
			if (physicalOperator.isReady()) {
				// KT FIX - call this only if necessary
				physicalOperator.setResultDocument(doc);
				// Put the new created physical operator in the operator queue
				opQueue.putOperator(physicalOperator);
			}
			nodesScheduled.put(node, physicalOperator);
		}
	}

	private void setStreamFlags(SchedulablePlan node) {
		int numInputs = node.getArity();

		// we want to do this processing bottom up, so
		// make recursive call first, then take action
		SchedulablePlan childPlan = null;
		for (int child = 0; child < numInputs; child++) {
			childPlan = node.getInput(child);
			setStreamFlags(childPlan);
		}

		if (numInputs == 1 && childPlan.isSendImmediate()) {
			node.setSendImmediate();
		}
	}
}
