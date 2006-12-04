/* $Id: DBThread.java,v 1.1 2006/12/04 21:13:30 tufte Exp $ */

package niagara.data_manager;

/**
 * Niagra DataManager DBThread - an input thread to connect to a database,
 * execute a query and put the results into a stream of tuples that can be
 * processed by the database
 */

import java.lang.reflect.Array;

import niagara.logical.DBScan;
import niagara.logical.DBScanSpec;
import niagara.optimizer.colombia.*;
import niagara.query_engine.TupleSchema;
import niagara.utils.*;

// for jdbc
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * DBThread provides a SourceThread compatible interface to fetch data from a
 * rdbms via jdbc
 */
public class DBThread extends SourceThread {

	// think these are optimization time??
	private DBScanSpec dbScanSpec;
	private Attribute[] variables;

	// these are run-time??
	private SinkTupleStream outputStream;
	private CPUTimer cpuTimer;

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
	 */
	public void opInitFrom(LogicalOp lop) {
		DBScan op = (DBScan) lop;
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
		if (o == null || !(o instanceof DBThread))
			return false;
		if (o.getClass() != getClass())
			return o.equals(this);
		// XXX vpapad: Spec.equals is Object.equals
		return dbScanSpec.equals(((DBThread) o).dbScanSpec);
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		DBThread dt = new DBThread();
		dt.dbScanSpec = dbScanSpec;
		dt.variables = variables;
		return dt;
	}

	public void plugIn(SinkTupleStream outputStream, DataManager dm) {
		this.outputStream = outputStream;
	}

	/**
	 * @see niagara.utils.SerializableToXML#dumpAttributesInXML(StringBuffer)
	 */
	public void dumpAttributesInXML(StringBuffer sb) {
		for (int i = 0; i < Array.getLength(variables); i++) {
			sb.append(" var='").append(variables[i].getName());
		}
		sb.append("'/>");
	}

	/**
	 * Thread run method
	 * 
	 */
	public void run() {

		try {
			Class.forName("com.mysql.jdbc.Driver");

		} catch (ClassNotFoundException e) {
			System.out.println("Class not found " + e.getMessage());
			return;
		}
		if (niagara.connection_server.NiagraServer.RUNNING_NIPROF)
			JProf.registerThreadName(this.getName());

		if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
			cpuTimer = new CPUTimer();
			cpuTimer.start();
		}

		// steps
		// connect to the database
		// execute the query
		// put the results into a tuple
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String qs = dbScanSpec.getQueryString();
			System.out.println("got to the thread query is:" + qs);

			System.out.println("Attempting to connect to server.");
			conn = DriverManager.getConnection(
					"jdbc:mysql://db.cecs.pdx.edu/tufte", "tufte", "loopdata");
			System.out.println("Connected.");

			stmt = conn.createStatement();
			System.out.println("Attempting to execute query.");
			rs = stmt.executeQuery(qs);
			System.out.println("Executed.");

			// Now do something with the ResultSet ....
			int numAttrs = dbScanSpec.getNumAttrs();
			System.out.println("Num Attrs: " + numAttrs);
			
			while (rs.next()) {
				//int a = rs.getInt("a");
				//int b = rs.getInt("b");
				//System.out.println(a + "   " + b);
				Tuple newtuple = new Tuple(false, numAttrs);
				for(int i = 1; i<= numAttrs; i++) {
					// get attribut type
					// then create the appropriate object
					// finally, load attribute into the tuple
					BaseAttr attr;
					switch(dbScanSpec.getAttrType(i-1)) {
					case Integer:
						System.out.println("Loaded integer");
						attr = new IntegerAttr();
						Integer iObj = new Integer(rs.getInt(i));
						attr.loadFromObject(iObj);
						break;
					case String:
						attr = new StringAttr();
						attr.loadFromObject(rs.getString(i));
						System.out.println("Loaded string");
						break;
					case XML:
						attr = new XMLAttr();
						attr.loadFromObject(rs.getString(i));
						System.out.println("Loaded xmlattr");
						break;
					default:
						throw new PEException("Invalid type " + dbScanSpec.getAttrType(i));
					}
					
					newtuple.appendAttribute(attr);
				}  // end for
				outputStream.putTuple(newtuple);
			} // end while
			// or alternatively, if you don't know ahead of time that
			// the query will be a SELECT...
			// if (stmt.execute("SELECT foo FROM bar")) {
			// rs = stmt.getResultSet();
			// }
			
			outputStream.endOfStream();
			
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			try {
				outputStream.putCtrlMsg(CtrlFlags.SHUTDOWN, ex.getMessage());
			} catch (Exception e) {
				// just ignore it
			}
		} catch(ShutdownException se) {
			// no need to send shutdown, we must have got it from the output stream
		} catch(InterruptedException ie) {
			try {
				outputStream.putCtrlMsg(CtrlFlags.SHUTDOWN, ie.getMessage());
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

	}	// end of run method

	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		;
	}
}
