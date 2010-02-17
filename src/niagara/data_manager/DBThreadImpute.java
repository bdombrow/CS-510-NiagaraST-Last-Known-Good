package niagara.data_manager;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import niagara.connection_server.NiagraServer;
import niagara.logical.DBScanImpute;
import niagara.logical.DBScanSpec;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.IntegerAttr;
import niagara.utils.JProf;
import niagara.utils.LongAttr;
import niagara.utils.PEException;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;
import niagara.utils.StringAttr;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;
import niagara.utils.XMLAttr;

/***
 * Data Streams Final Project changes: This version of DBThread is trimmed down
 * (no "latte demo" * support is left), and it is tweaked to respond to the
 * imputation mechanism proposed for class. --RJFM
 * 
 * 
 * Niagra DataManager DBThread - an input thread to connect to a database,
 * execute a query and put the results into a stream of tuples that can be
 * processed by the database
 * 
 * 
 * DBThread provides a SourceThread compatible interface to fetch data from a
 * rdbms via jdbc
 */
@SuppressWarnings("unchecked")
public class DBThreadImpute extends SourceThread {
	// some truth we all know;

	public static final int MILLISEC_PER_SEC = 1000;

	// think these are optimization time??
	public DBScanSpec dbScanSpec;

	private Attribute[] variables;

	// database connection;
	private Connection conn = null;

	private Statement stmt = null;

	// these are run-time??
	public SinkTupleStream outputStream;

	private ArrayList newQueries;

	private boolean finish = false;

	private boolean shutdown = false;

	// Just keeps information on query string...
	private class Query {
		public Query(String query) {
			queryStr = query;
		}

		String queryStr;
	}

	enum Status {
		FUTURE, DONE, PROGRESS
	};

	public DBThreadImpute() {
		newQueries = new ArrayList();
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
	 */
	public void opInitFrom(LogicalOp lop) {
		DBScanImpute op = (DBScanImpute) lop;
		dbScanSpec = op.getSpec();
		variables = op.getVariables();
	}

	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		;
	}

	public TupleSchema getTupleSchema() {
		TupleSchema ts = new TupleSchema();
		ts.addMappings(new Attrs(variables, Array.getLength(variables)));
		return ts;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		// XXX vpapad: totally bogus flat cost for stream scans
		return new Cost(catalog.getDouble("stream_scan_cost"));
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		// XXX vpapad: spec's hashCode is Object.hashCode()
		return dbScanSpec.hashCode();
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object o) {
		if (o == null || !(o instanceof DBThreadImpute))
			return false;
		if (o.getClass() != getClass())
			return o.equals(this);
		// XXX vpapad: Spec.equals is Object.equals
		return dbScanSpec.equals(((DBThreadImpute) o).dbScanSpec);
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		DBThreadImpute dt = new DBThreadImpute();
		dt.dbScanSpec = dbScanSpec;
		dt.variables = variables;
		return dt;
	}

	public void plugIn(SinkTupleStream outputStream, DataManager dm) {
		this.outputStream = outputStream;
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" var='");
		for (int i = 0; i < variables.length; i++) {
			sb.append(variables[i].getName());
			if (i != variables.length - 1)
				sb.append(", ");
		}
		sb.append("'/>");
	}

	/**
	 * Thread run method
	 * 
	 */
	public void run() {

		try {
			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {
			System.out.println("Class not found " + e.getMessage());
			return;
		}
		if (niagara.connection_server.NiagraServer.RUNNING_NIPROF)
			JProf.registerThreadName(this.getName());

		// steps
		// connect to the database
		// execute the query
		// put the results into a tuple
		ResultSet rs = null;

		try {
			System.out.println("Attempting to connect to server.");
			conn = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/latte", "postgres", "");

			System.out.println("Connected to database.");

			stmt = conn.createStatement();

			stmt.execute("SET work_mem=100000");
			String qs;

			// one time db query;
			if (dbScanSpec.oneTime()) {
				qs = dbScanSpec.getQueryString();
				// System.out.println("query string: " + qs);
				// qs = qs.replace("FOO", "loopdata"); // This is just to test
				// whether replace works.

				// System.out.println("DB Query is:" + qs);
				// System.out.println("Attempting to execute one-time query.");

				// Query q;

				rs = stmt.executeQuery(qs);
				// System.out.println("Executed.");

				// Now do something with the ResultSet ....
				int numAttrs = dbScanSpec.getNumAttrs();
				// System.out.println("Num Attrs: " + numAttrs);

				// putResults(rs, numAttrs);
				putResults(rs, numAttrs);
				outputStream.endOfStream();

				// System.out.println("I put the results...");

			} else
				do { // continuous db scan
					/*
					 * response to query shutdown;
					 */
					if (shutdown) {
						outputStream.putCtrlMsg(ControlFlag.SHUTDOWN,
								"bouncing back SHUTDOWN msg");

						break;
					}

					checkForSinkCtrlMsg(-1);

					/*
					 * if this is a continuous db scan
					 */
					if (newQueries.size() == 0) {
						if (finish) {
							outputStream.endOfStream();
							break;
						} else {
							checkForSinkCtrlMsg(1 * MILLISEC_PER_SEC);
							if (newQueries.size() == 0)
								continue;
						}
					}

					// System.err.println("***DID NOT GO THERE;");

					stmt = conn.createStatement();
					if (NiagraServer.DEBUG)
						System.out
								.println("Attempting to execute continuous query.");

					Query curQuery = (Query) newQueries.remove(0);
					if (NiagraServer.DEBUG)
						System.err.println(curQuery.queryStr);

					rs = stmt.executeQuery(curQuery.queryStr);
					// System.out.println("Executed query");
					checkAllSinkCtrlMsg();
					// Now do something with the ResultSet ....
					int numAttrs = dbScanSpec.getNumAttrs();

					putResults(rs, numAttrs);

				} while (true);

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			try {
				outputStream.putCtrlMsg(ControlFlag.SHUTDOWN, ex.getMessage());
			} catch (Exception e) {
				// just ignore it
			}
		} catch (ShutdownException se) {
			try {
				// outputStream.endOfStream();
			} catch (Exception e) {
				// ignore it
			}
			// no need to send shutdown, we must have got it from the output
			// stream
		} catch (InterruptedException ie) {
			try {
				outputStream.putCtrlMsg(ControlFlag.SHUTDOWN, ie.getMessage());
			} catch (Exception e) {
				// just ignore it
			}
		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) { // ignore
					rs = null;
				}
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) { // ignore
					stmt = null;
				}
			}

			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException sqlEx) { // ignore
					conn = null;
				}
			}
		}// end of finally

	} // end of run method

	private void putResults(ResultSet rs, int numAttrs) throws SQLException,
			ShutdownException, InterruptedException {

		while (rs.next()) {
			Tuple newtuple = new Tuple(false, numAttrs);
			for (int i = 1; i <= numAttrs; i++) {
				// get attribut type
				// then create the appropriate object
				// finally, load attribute into the tuple
				BaseAttr attr;
				switch (dbScanSpec.getAttrType(i - 1)) {
				case Int:
					attr = new IntegerAttr();
					Integer iObj = new Integer(rs.getInt(i));
					attr.loadFromObject(iObj);
					break;
				case Long:
					attr = new LongAttr();
					Long lObj = new Long(rs.getLong(i));
					attr.loadFromObject(lObj);
					break;
				case TS:
					attr = new TSAttr();
					Long tObj = new Long(rs.getLong(i));
					attr.loadFromObject(tObj);
					break;
				case String:
					attr = new StringAttr();
					attr.loadFromObject(rs.getString(i));
					break;
				case XML:
					attr = new XMLAttr();
					attr.loadFromObject(rs.getString(i));
					break;
				default:
					throw new PEException("Invalid type "
							+ dbScanSpec.getAttrType(i));
				}

				newtuple.appendAttribute(attr);
			} // end for
			ArrayList ctrl = outputStream.putTuple(newtuple);
			while (ctrl != null) {
				processCtrlMsgFromSink(ctrl);
				ctrl = outputStream.flushBuffer();
				// assert ctrlFlag == CtrlFlags.NULLFLAG: "bad ctrl flag ";
			}

		} // end while

	}

	public void checkForSinkCtrlMsg(int timeout)
			throws java.lang.InterruptedException, ShutdownException,
			SQLException {
		// Loop over all sink streams, checking for control elements
		ArrayList ctrl;

		// Make sure the stream is not closed before checking is done
		if (!outputStream.isClosed()) {
			ctrl = outputStream.getCtrlMsg(timeout);
			// ctrl = outputStream.getCtrlMsg(1*MILLISEC_PER_SEC);

			// If got a ctrl message, process it
			if (ctrl != null) {
				processCtrlMsgFromSink(ctrl);
			}
		}
	}

	private void checkAllSinkCtrlMsg() throws java.lang.InterruptedException,
			ShutdownException, SQLException {

		ArrayList ctrlMsgList = new ArrayList();

		// Make sure the stream is not closed before checking is done
		if (!outputStream.isClosed()) {
			ArrayList ctrl;
			do {
				ctrl = outputStream.getCtrlMsg(-1);

				// If got a ctrl message, process it
				if (ctrl != null) {
					ctrlMsgList.add(ctrl);
				}
			} while (ctrl != null);
		}
		for (Object ctrl : ctrlMsgList)
			processCtrlMsgFromSink((ArrayList) ctrl);
	}

	/*
	 * RJFM: Modified
	 * 
	 * Presumably this method schedules a new query to be executed...
	 */

	public void processCtrlMsgFromSink(ArrayList ctrl) throws SQLException,
			ShutdownException, InterruptedException {
		if (ctrl == null) {
			System.err.println("I am null :-(");
			return;
		}

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);
		String ctrlMsg = (String) ctrl.get(1);
		switch (ctrlFlag) {
		case IMPUTE:
			System.err.println("Received Flag");
			newQueries.add("SELECT 1,1,impute_value,1 FROM impute_value(1,1)");
			break;
		case MESSAGE:
			// System.err.println("I am " + this.getName() +
			// " and I received this message: " + ctrl.get(1));
			// if (newQueries.size() < 30){
			// CASE B newQueries = new ArrayList();
			break;
		case CHANGE_QUERY:

			/*
			 * XXX RJFM: We perform all query processing here.
			 * 
			 * First, upon receiving a CHANGE_QUERY flag, we extract the
			 * relevant features from the tokenized control message created from
			 * PunctQC. Second, we rewrite the query substituting the
			 * appropriate parameters. Third, add the newly constructed query to
			 * the queue.
			 */

			// System.err.println("Received Imputation Flag");

			// XXX Extract values
			// var[0] = timestamp
			// var[1] = speed at station_a
			// var[3] = speed at station_c
			String[] var;
			var = new String[5];
			int i = 0;
			StringTokenizer token = new StringTokenizer(ctrlMsg, "#");
			while (token.hasMoreTokens()) {
				var[i] = token.nextElement().toString();
				// System.out.println("var["+i+"]-> "+var[i]);
				i++;
			}

			Query q = new Query(dbScanSpec.getQueryString());
			q.queryStr = (q.queryStr).replace("DATA", "'" + var[0] + "', "
					+ var[1] + ", imputed_value, " + var[3]);
			// q.queryStr = (q.queryStr).replace("DATA", "'" + var[0] + "', " +
			// var[1] + ", imputed_value2, " + var[3]);
			q.queryStr = (q.queryStr).replace("SOURCE", "imputed_value( "
					+ var[1] + "," + var[3] + ")");
			// q.queryStr = (q.queryStr).replace("SOURCE", "imputed_value2( " +
			// var[1] + "," + var[3] + ",10)");
			// System.err.println("query:" + q.queryStr);
			newQueries.add(q);
			break;
		case READY_TO_FINISH:
			System.err.println("live stream ends...");
			finish = true;
			break;
		case SHUTDOWN: // handle query shutdown;
			System.err.println("ready to shutdown at DBThreadImpute");
			shutdown = true;
			break;
		default:
			assert false : "KT unexpected control message from source "
					+ ctrlFlag.flagName();
		}
	}

	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		;
	}

}
