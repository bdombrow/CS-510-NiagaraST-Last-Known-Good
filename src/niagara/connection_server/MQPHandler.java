/**
 * $Id: MQPHandler.java,v 1.3 2002/05/07 03:10:34 tufte Exp $
 *
 */

package niagara.connection_server;

import niagara.query_engine.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

import java.io.*;
import java.util.*;
import java.net.*;

public class MQPHandler extends Thread {
    ExecutionScheduler es;
    logNode root;
    Vector resources;
    Vector schedulableRoots;
    StreamMaterializer[] subplans;
    String[] results;
    Catalog catalog;
    int subPlansDone;

    public MQPHandler(ExecutionScheduler es, logNode root) {
        this.es = es;
        this.root = root;
        subPlansDone = 0;
    }

    public void run() {
	try {
	    catalog = NiagraServer.getCatalog();
	    
	    // Replace all resource nodes we know about with
	    // dtdscans
	    resources = new Vector();
	    Vector allNodes = new Vector();
	    Vector traverse = new Vector();
	    
	    traverse.add(root);
	    
	    while (!traverse.isEmpty()) {
		logNode n = (logNode) traverse.remove(0);
		if (allNodes.contains(n))
		    continue;
		if (n.getOperator() instanceof ResourceOp) {
		    resources.add(n);
		}
		else {
		    logNode[] inputs = n.getInputs();
		    for (int i = 0; i < inputs.length; i++) {
			traverse.add(inputs[i]);
		    }
		}
		allNodes.add(n);
	    }
	    
	    for (int i = 0; i < resources.size(); i++) {
		logNode node = (logNode) resources.get(i);
		ResourceOp rop = (ResourceOp) node.getOperator();
		String urn = rop.getURN();
		if (catalog.isLocallyResolvable(urn)) {
		    dtdScanOp op = null;
		    try {
			op = (dtdScanOp) operators.DtdScan.clone();
		    } catch (CloneNotSupportedException e) {
			throw new PEException("Unable to clone DTDScan " +
					      e.getMessage()); 
		    }
		    op.setDocs(catalog.getURL(urn));
		    node.setOperator(op);
		    resources.remove(node);
		}
	    }
	    
	    // Maybe now the root is schedulable?
	    if (root.isSchedulable()) {
		es.scheduleSubPlan(root);
		return;
	    }
            
	    // Find roots of subgraphs that are schedulable
	    schedulableRoots = new Vector(); 
	    
	    // non-schedulable nodes to check
	    Vector toCheck = new Vector(); 
	    
	    toCheck.add(root);
	    
	    // Do a depth first traversal of the query graph
	    // trying to find the roots of schedulable subgraphs
	    while (!toCheck.isEmpty()) {
		logNode n = (logNode) toCheck.remove(0);
		logNode[] inputs = n.getInputs();
		for (int i = 0; i < inputs.length; i++) {
		    logNode m = inputs[i];
		    if (m.isSchedulable())
			schedulableRoots.add(m);
		    else
			toCheck.add(m);
		}
	    }
	    
	    subplans =  new StreamMaterializer[schedulableRoots.size()];
	    results = new String[schedulableRoots.size()];
	    
	    for (int i = 0; i < subplans.length; i++) {
		PageStream is = 
		    es.scheduleSubPlan((logNode) schedulableRoots.get(i));
		subplans[i] = new StreamMaterializer(i, this, 
						     new SourceTupleStream(is));
		subplans[i].start();
	    }
	    
	    if (subplans.length == 0)
		route();
	} catch (ShutdownException se) {
	    // nothing to do but print a warning and return
	    System.err.println("MQPHandler got shutdown "+se.getMessage());
	    return;
	}
    }

    public synchronized void subPlanDone(int number, String result) {
        results[number] = result;
        subPlansDone++;
        if (subPlansDone < subplans.length)
            return;

        // Build new MQP
        for (int k = 0; k < schedulableRoots.size(); k++) {
            logNode n = (logNode) schedulableRoots.get(k);
            ConstantOp cop = null;
            try {
                cop = (ConstantOp) operators.constantOp.clone();
	    } catch (CloneNotSupportedException ex) { 
		throw new PEException("cant clone constant op " + ex.getMessage());
	    }
            cop.setContent(results[number]);
            cop.setVars(n.getVarTbl().getVars());
            n.setOperator(cop);
            n.setInputs(new logNode[] {});
        }

        route();
    }
    
    public void route() {
        // Decide on the next node to send the MQP to
        Hashtable votes = new Hashtable();
        for (int i = 0; i < resources.size(); i++) {
            logNode node = (logNode) resources.get(i);
            ResourceOp rop = (ResourceOp) node.getOperator();
            String urn = rop.getURN();
            Vector resolvers = catalog.getResolvers(urn);
            if (resolvers == null)
                continue;
            for (int j = 0; j < resolvers.size(); j++) {
                String location = (String) resolvers.get(j);
                if (!votes.containsKey(location)) {
                    votes.put(location, new Integer(1));
                }
                else {
                    int currentVotes = ((Integer)
                                        votes.get(location)).intValue();
                    currentVotes++;
                    votes.put(location,new Integer(currentVotes));
                }
            }
        }
        int maxVotes = 0;
        String location = "";
        Enumeration e = votes.keys();
        while (e.hasMoreElements()) {
            String newLocation = (String) e.nextElement();
            int currentVotes = ((Integer)
                                votes.get(newLocation)).intValue();
            if (currentVotes > maxVotes) {
                maxVotes = currentVotes;
                location = newLocation;
            }
        }
            
        if (location.equals("")) {
            System.err.println("Could not find next hop for MQP!");
        }
        else {
            // Send MQP to next hop
            String url_location = "http://" + location 
                + "/servlet/communication";
            try {
                String encodedQuery = URLEncoder.encode(root.planToXML());
                
                URL url = new URL(url_location);
                URLConnection connection = url.openConnection();
                connection.setDoOutput(true);
                PrintWriter out = new PrintWriter(connection.getOutputStream());
                out.println("type=submit_plan&query=" + encodedQuery);
                out.close();
                
                connection.getInputStream().close();
                Date d = new Date();
                System.out.println("Mutant Query routed: " + d.getTime() % (60 * 60 * 1000));
            }
            catch (Exception ex) {
                System.out.println("Exception while sending MQP to server:" + location);
                ex.printStackTrace();
                System.exit(-1);
            }
        }
    }
}






