package niagara.query_engine;

import niagara.physical.PhysicalOperator;
import niagara.utils.JProf;

/////////////////////////////////////////////////////////////////////////
//
//   OperatorThread.java:
//   NIAGRA Project
//   
//   Jayavel Shanmugasundaram
//
//   This is the Operator Thread class used to run all the operators. Each  
//   instance of this class block on an operator queue and gets the next
//   scheduled operator and runs it.
//
//
//   ---------------
//   Methods
//   ---------------
//
//   run()                  // For implementing the interface Runnable (thread)
//
//
//   ---------------
//   Data members
//   ---------------
//   Thread thread          // The Java Thread associated with the operator thread
//   OperatorQueue opQueue  // The Queue from which operators are removed
//
/////////////////////////////////////////////////////////////////////////

public class PhysicalOperatorThread implements Runnable {
	// /////////////////////////////////////////////////
	// Data members of the PhysicalOperatorThread Class
	// /////////////////////////////////////////////////

	// The thread associated with the class
	private Thread thread;

	// The operator queue on which the OperatorThread is to wait
	private PhysicalOperatorQueue opQueue;

	// /////////////////////////////////////////////////
	// Methods of the Operator Class
	// /////////////////////////////////////////////////

	/**
	 * This is the constructor for the OperatorThread class that is initialized
	 * with an OperatorQueue to wait on. A Java thread is created and is
	 * associated with the Operator Thread.
	 * 
	 * @param opQueue
	 *            The name of the operator queue with which the thread
	 *            communicates.
	 */

	public PhysicalOperatorThread(PhysicalOperatorQueue opQueue) {
		super();

		// Initialize the Operator Queue
		this.opQueue = opQueue;

		// Create a new java thread for running an instance of this object
		thread = new Thread(this, "OpThreadUnused");
		thread.setDaemon(true);
		thread.start();
	}

	/**
	 * This is the run method invoked by the Java thread
	 */
	public void run() {
		// Keep waiting on the Operator Queue until there is a new operator to
		// be scheduled. Then once an operator is obtained, run it to
		// completion.
		// Then repeat the process.
		//
		do {
			// Get an operator for running
			Schedulable op = opQueue.getOperator();

			// Execute the operator
			assert op != null : "KT op is null";
			assert op.getName() != null : "KT op name is null"
					+ op.getClass().getName();
			thread.setName(op.getName());
			// KAT check here
			if (niagara.connection_server.NiagraServer.RUNNING_NIPROF
					&& op instanceof PhysicalOperator) {
				PhysicalOperator physOp = (PhysicalOperator) op;
				JProf.registerThreadName(physOp.getName() + "("
						+ physOp.getId() + ")");
			}
			op.run();

			// Garbage collect the memory occupied by the operator
			// *now*, instead of waiting for the next time this thread
			// executes an operator
			op = null;
		} while (true);
	}
}
