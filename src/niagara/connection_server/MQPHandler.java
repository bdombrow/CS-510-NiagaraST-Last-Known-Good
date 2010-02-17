package niagara.connection_server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import niagara.client.LightMQPClient;
import niagara.logical.ConstantScan;
import niagara.logical.Display;
import niagara.logical.Resource;
import niagara.optimizer.Optimizer;
import niagara.optimizer.Plan;
import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.ExecutionScheduler;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;
import niagara.utils.SourceTupleStream;

@SuppressWarnings("unchecked")
public class MQPHandler {
	ExecutionScheduler es;
	Plan root;

	ArrayList schedulableRoots;
	ArrayList resources;

	StreamMaterializer[] subplans;
	String[] results;
	Catalog catalog;
	int subPlansDone;

	public MQPHandler(ExecutionScheduler es, Optimizer optimizer, Plan p) {
		this.es = es;
		this.root = p;
		this.catalog = NiagraServer.getCatalog();

		subPlansDone = 0;
		schedulableRoots = new ArrayList();
		resources = new ArrayList();

		root = optimizer.consolidate(root.toExpr());

		try {
			root.getRootsAndResources(new HashSet(), schedulableRoots,
					resources, true);

			// If the root is schedulable, just schedule it and we're done
			if (schedulableRoots.size() == 1 && root == schedulableRoots.get(0)) {
				Plan optimizedPlan = optimizer.optimize(root.toExpr());
				es.scheduleSubPlan(optimizedPlan);
				return;
			}

			// If there is nothing we can schedule, route the plan
			if (schedulableRoots.size() == 0)
				route();

			subplans = new StreamMaterializer[schedulableRoots.size()];
			results = new String[schedulableRoots.size()];

			for (int i = 0; i < subplans.length; i++) {
				Plan optimizedPlan = optimizer
						.optimize(((Plan) schedulableRoots.get(i)).toExpr());

				((Plan) schedulableRoots.get(i)).replaceWith(optimizedPlan);
				PageStream is = es.scheduleSubPlan(optimizedPlan);
				subplans[i] = new StreamMaterializer(i, this,
						new SourceTupleStream(is));
				subplans[i].start();
			}

		} catch (ShutdownException se) {
			// nothing to do but print a warning and return
			System.err.println("MQPHandler got shutdown " + se.getMessage());
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
			Plan p = (Plan) schedulableRoots.get(k);
			ConstantScan cop = new ConstantScan(results[number], new Attrs(p
					.getTupleSchema().getVariables()));
			p.setOperator(cop);
			p.setInputs(new Plan[] {});
		}

		route();
	}

	public void route() {
		// Decide on the next node to send the MQP to
		HashMap votes = new HashMap();
		for (int i = 0; i < resources.size(); i++) {
			Plan node = (Plan) resources.get(i);
			Resource rop = node.getResource();
			String urn = rop.getURN();
			Vector resolvers = catalog.getResolvers(urn);
			if (resolvers == null)
				continue;
			for (int j = 0; j < resolvers.size(); j++) {
				String location = (String) resolvers.get(j);
				if (!votes.containsKey(location)) {
					votes.put(location, new Integer(1));
				} else {
					int currentVotes = ((Integer) votes.get(location))
							.intValue();
					currentVotes++;
					votes.put(location, new Integer(currentVotes));
				}
			}
		}
		int maxVotes = 0;
		String location = "";
		Iterator i = votes.keySet().iterator();
		while (i.hasNext()) {
			String newLocation = (String) i.next();
			int currentVotes = ((Integer) votes.get(newLocation)).intValue();
			if (currentVotes > maxVotes) {
				maxVotes = currentVotes;
				location = newLocation;
			}
		}

		// Try to send the MQP to the next server
		String plan = root.planToXML();

		if (location.length() == 0) {
			System.err.println("Could not find next hop for MQP!");
		} else if (sendTCP(plan, location)) {
			// We're done
			return;
		} else
			System.err.println("Unable to route MQP to server: " + location);

		// Delivery failed, route back to client
		Display d = (Display) root.getOperator();
		// If that fails too, give up
		if (!sendHTTP(plan, d.getClientLocation()))
			System.err.println("Could not return undeliverable MQP to client");
	}

	private boolean sendTCP(String plan, String location) {
		int colonPos = location.indexOf(':');
		String host = location.substring(0, colonPos);
		String port = location.substring(colonPos + 1, location.length());
		LightMQPClient mc = new LightMQPClient(host, Integer.parseInt(port));
		if (mc.getErrorMessage() != null)
			return false;
		mc.sendQuery(plan, 500);
		return (mc.getErrorMessage() == null);
	}

	private boolean sendHTTP(String plan, String location) {
		try {
			String result = root.planToXML();
			String query_id = String.valueOf(((Display) root.getOperator())
					.getQueryId());
			URL url = new URL(location);
			URLConnection connection = url.openConnection();
			connection.setDoOutput(true);
			PrintWriter out = new PrintWriter(connection.getOutputStream());
			out.println(query_id);
			out
					.println("<!-- The server could not completely evaluate this query -->");
			out.println(result);
			out.close();

			connection.getInputStream().close();
			// XXX vpapad: log that plan was routed
		} catch (MalformedURLException mue) {
			return false;
		} catch (IOException ioe) {
			return false;
		}
		return true;
	}
}
