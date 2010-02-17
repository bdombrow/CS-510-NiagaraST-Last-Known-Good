package niagara.data_manager;

import niagara.connection_server.NiagraServer;
import niagara.logical.Load;
import niagara.ndom.saxdom.SAXDOMReader;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.CPUTimer;
import niagara.utils.ControlFlag;
import niagara.utils.JProf;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;

import org.xml.sax.SAXException;

public class LoadThread extends SourceThread {
	// Runtime variables
	private SAXDOMReader sr;
	private DataManager dm;
	private SinkTupleStream outputStream;
	private CPUTimer cpuTimer;

	// Optimization-time attributes
	private Attribute variable;
	private String resource;

	public void opInitFrom(LogicalOp lop) {
		Load op = (Load) lop;
		resource = op.getResource();
		variable = op.getVariable();
	}

	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		;
	}

	public TupleSchema getTupleSchema() {
		TupleSchema ts = new TupleSchema();
		ts.addMapping(variable);
		return ts;
	}

	/**
	 * Thread run method
	 * 
	 */
	public void run() {
		if (NiagraServer.RUNNING_NIPROF)
			JProf.registerThreadName(this.getName());

		if (NiagraServer.TIME_OPERATORS) {
			cpuTimer = new CPUTimer();
			cpuTimer.start();
		}

		boolean shutdown = false;
		String message = "normal";

		try {
			sr = new SAXDOMReader(dm.getInputStreamFor(resource));
			sr.readDocuments(outputStream);
		} catch (SAXException saxE) {
			System.err
					.println("StreamThread::SAX exception parsing document. Message: "
							+ saxE.getMessage());
			shutdown = true;
			message = "SAX Exception " + saxE.getMessage();
		} catch (java.io.IOException ioe) {
			System.err.println("StreamThread::IOException. Message: "
					+ ioe.getMessage());
			shutdown = true;
			message = "StreamThread::IOException " + ioe.getMessage();
		} catch (ShutdownException se) {
			System.err.println("StreamThread::ShutdownException. Message "
					+ se.getMessage());
			shutdown = true;
			message = se.getMessage();
		}

		cleanUp(shutdown, message);
		return;
	}

	private void cleanUp(boolean shutdown, String message) {
		try {
			sr.done();
		} catch (ShutdownException se) {
			// Nothing to do at this point, we're already shutting down
		}

		if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
			cpuTimer.stop();
			cpuTimer.print(getName() + "(shutdown: " + message + ")");
		}

		try {
			if (!shutdown)
				outputStream.endOfStream();
			else
				// REFACTOR
				outputStream.putCtrlMsg(ControlFlag.SHUTDOWN, message);
		} catch (java.lang.InterruptedException ie) {
			/* do nothing */
		} catch (ShutdownException se) {
			/* do nothing */
		}

		outputStream = null;
		sr = null;
		return;
	}

	public void plugIn(SinkTupleStream outputStream, DataManager dm) {
		this.outputStream = outputStream;
		this.dm = dm;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		// XXX vpapad: totally bogus flat cost for stream scans
		return new Cost(catalog.getDouble("stream_scan_cost"));
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof LoadThread))
			return false;
		if (o.getClass() != getClass())
			return o.equals(this);
		return equalsNullsAllowed(variable, ((LoadThread) o).variable)
				&& equalsNullsAllowed(resource, ((LoadThread) o).resource);
	}

	public int hashCode() {
		return hashCodeNullsAllowed(variable) ^ hashCodeNullsAllowed(resource);
	}

	public Op opCopy() {
		LoadThread st = new LoadThread();
		st.resource = resource;
		st.variable = variable;
		return st;
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" variable='").append(variable.getName());
		sb.append(" resource='").append(resource);
		sb.append("'/>");
	}

	public void dumpChildrenInXML(StringBuffer sb) {
		;
	}
}
