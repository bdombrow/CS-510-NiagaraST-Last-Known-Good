/* $Id: DBThread.java,v 1.3 2007/04/28 21:23:00 jinli Exp $ */

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
import niagara.utils.BaseAttr.Type;

// for jdbc
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 * DBThread provides a SourceThread compatible interface to fetch data from a
 * rdbms via jdbc
 */
public class DBThread extends SourceThread {

	// think these are optimization time??
	public DBScanSpec dbScanSpec;
	private Attribute[] variables;

	// these are run-time??
	public SinkTupleStream outputStream;
	public CPUTimer cpuTimer;
	private ArrayList newQueries;
	private boolean finish = false;
	private boolean shutdown = false; 
	
	private boolean  test = false;

	public DBThread() {
		newQueries = new ArrayList();
	}
	
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
		if (dbScanSpec.equals(((DBThread) o).dbScanSpec))
			System.err.println("DBSCAN equals true");
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
			Class.forName("org.postgresql.Driver");
	
		} catch (ClassNotFoundException e) {
			System.out.println("Class not found " + e.getMessage());
			return;
		}
		if (niagara.connection_server.NiagraServer.RUNNING_NIPROF)
			JProf.registerThreadName(this.getName());
	
		if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
			this.cpuTimer = new CPUTimer();
			this.cpuTimer.start();
		}
	
		// steps
		// connect to the database
		// execute the query
		// put the results into a tuple
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String timeConstraint;
		try {
			System.out.println("Attempting to connect to server.");
			//conn = DriverManager.getConnection("jdbc:postgresql://barista.cs.pdx.edu/latte", "tufte", "loopdata");
			Properties properties = new Properties();
			properties.setProperty("user", "tufte");
			properties.setProperty("password", "loopdata");

			conn = DriverManager.getConnection("jdbc:postgresql://barista.cs.pdx.edu/latte", properties);

			System.out.println("Connected.");
			
			stmt = conn.createStatement();

			stmt.execute("SET work_mem=100000");
			
			String qs = dbScanSpec.getQueryString();
			System.out.println("main query string: "+qs);
	
		do {
			if (dbScanSpec.oneTime()) {
				System.out.println("DB Query is:" + qs);
				//stmt = conn.createStatement();
				System.out.println("Attempting to execute query.");

				rs = stmt.executeQuery(qs);
				//System.out.println("Executed.");				
		
				// Now do something with the ResultSet ....
				int numAttrs = dbScanSpec.getNumAttrs();
				//System.out.println("Num Attrs: " + numAttrs);
				
				putResults(rs, numAttrs);

				outputStream.endOfStream();
				break;
			}
			
			/*
			 * response to query shutdown;
			 */
			if (shutdown) {
				outputStream.putCtrlMsg(CtrlFlags.SHUTDOWN, "bouncing back SHUTDOWN msg");
				
				break;
			}
				
			/*
			 * if this is a continuous db scan
			 */	
			if (newQueries.size()==0) {
				if (finish){
					outputStream.endOfStream();
					break;
				} else {
					//blocking call to get ctrl msg;
					checkForSinkCtrlMsg();
					
					if (newQueries.size()==0)
						continue;
				}
			}
				
			//timeConstraint = changeQuery((String) (newQueries.remove(0)));
			//System.out.println("DB Query is:" + qs+timeConstraint);
	
			stmt = conn.createStatement();
			System.out.println("Attempting to execute query.");
			
			if (!test) {
				/*timeConstraint = changeQuery((String) (newQueries.remove(0)));
				System.out.println("DB Query is:" + qs+timeConstraint);

				rs = stmt.executeQuery(qs+timeConstraint);*/
				String queryString = (String) newQueries.remove(0);
				System.err.println(queryString);
				rs = stmt.executeQuery(queryString);
		
				// Now do something with the ResultSet ....
				int numAttrs = dbScanSpec.getNumAttrs();
								
				putResults(rs, numAttrs);
				
			} else {
				timeConstraint = (String) (newQueries.remove(0));
				System.out.println("DB Query is:" + qs+timeConstraint);

				for (int j = 1000; j<=2000; j++) {
					String sensor = " and detectorid= "+j;
			
					rs = stmt.executeQuery(qs+timeConstraint+sensor);
					//System.out.println("Executed.");				

					// Now do something with the ResultSet ....
					int numAttrs = dbScanSpec.getNumAttrs();
					//System.out.println("Num Attrs: " + numAttrs);
					
					putResults(rs, numAttrs);
				} // end for
			}
			
		} while (true);
			
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
			try {
			//outputStream.endOfStream();
			} catch (Exception e) {
				//ignore it
			}
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
	
	private void putResults (ResultSet rs, int numAttrs) 
		throws SQLException, ShutdownException, InterruptedException {

		//rs.beforeFirst();
		System.err.println("fetch direction: "+rs.getFetchDirection());
		if (rs.isBeforeFirst())
			System.err.println("yes, we are at the start of rows ********************");
		else
			System.err.println("no, we aren't at the start ********************");
		while (rs.next()) {
			Tuple newtuple = new Tuple(false, numAttrs);
			for(int i = 1; i<= numAttrs; i++) {
				// get attribut type
				// then create the appropriate object
				// finally, load attribute into the tuple
				BaseAttr attr;
				switch(dbScanSpec.getAttrType(i-1)) {
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
					throw new PEException("Invalid type " + dbScanSpec.getAttrType(i));
				}
				
				newtuple.appendAttribute(attr);
			}  // end for
			ArrayList ctrl = outputStream.putTuple(newtuple);
			while(ctrl != null) {
				processCtrlMsgFromSink(ctrl);
				ctrl = outputStream.flushBuffer();
				//assert ctrlFlag == CtrlFlags.NULLFLAG: "bad ctrl flag ";
			}
			
		} // end while				

	}
	
    public void checkForSinkCtrlMsg()
	throws java.lang.InterruptedException, ShutdownException {
	// Loop over all sink streams, checking for control elements
    ArrayList ctrl;

    // Make sure the stream is not closed before checking is done
    if (!outputStream.isClosed()) {
    	ctrl = outputStream.getCtrlMsg(1*1000);
	    
    	// If got a ctrl message, process it
    	if (ctrl != null) {
    		processCtrlMsgFromSink(ctrl);
    	} else {
    		
    	}
    	}
    }
    
    public void processCtrlMsgFromSink(ArrayList ctrl) 
    	throws ShutdownException {
    	if (ctrl == null)
    		return;
    	
    	int ctrlFlag = (Integer)ctrl.get(0); 
    	String ctrlMsg = (String)ctrl.get(1);
    	switch (ctrlFlag) {
    	case CtrlFlags.CHANGE_QUERY:
    		newQueries.add(ctrlMsg);
    		break;
    	case CtrlFlags.READY_TO_FINISH:
    		System.err.println("live stream ends...");
    		finish = true;
    		break;
    	case CtrlFlags.SHUTDOWN: //handle query shutdown;
    		System.err.println("ready to shutdown at DBThread");
    		shutdown = true;
    		break;
    	default:
    		assert false : "KT unexpected control message from source " + 
		    CtrlFlags.name[ctrlFlag];
    	}
    }
    
    public String changeQuery (String queryPara) {
    	
    	String[] queryRange = queryPara.split("\t");
    	assert queryRange.length == 2: "Jenny - range is more than a pair?!";
    	String timeattr = dbScanSpec.getTimeAttr();
    	String timeFunct = timeattr; 
    	//StringBuffer query = new StringBuffer(" " + timeFunct  + ">= '" + queryRange[0] + "' and "+ timeFunct + "<= '" +queryRange[1]+"' ");
    	//query.append(" and dbdetectorid= "+queryRange[2]);
    	test = (queryRange[0].compareTo(queryRange[1]) == 0);
    	String query = " " + timeFunct  + ">= '" + queryRange[0] + "' and "+ timeFunct + "<= '" +queryRange[1]+"' ";
    	System.out.println("new query predicate: "+query);
    	return query;
    }


	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		;
	}
}
