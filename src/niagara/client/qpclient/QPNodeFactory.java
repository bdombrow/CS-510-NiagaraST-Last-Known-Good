/**
 * Class <code>QPNodeFactory</code> is a registry for query plan node
 * types.
 *
 * @author Vassilis Papadimos
 * @version 1.0
 */

package niagara.client.qpclient;

import java.util.HashMap;
import org.w3c.dom.*;
import diva.graph.model.*;

public class QPNodeFactory { 
    
    static HashMap names = new HashMap();

    // All element to QPNode mappings should be established here

    static {
	names.put("plan", Plan.class);
	names.put("scan", Scan.class);
	names.put("select", Select.class);
	names.put("join", Join.class);
	names.put("avg", Avg.class);
	names.put("count", Count.class);
	names.put("dtdscan", DtdScan.class);
	names.put("construct", Construct.class);
	names.put("firehosescan", FirehoseScan.class);
	names.put("expression", Expression.class);
	names.put("dup", Dup.class);
	names.put("accumulate", Accumulate.class);
    }

    /**
     * Creates a new <code>QPNode</code> instance, from a DOM element.
     *
     * @param e a DOM <code>Element</code>
     *
     * @return a new <code>QPNode</code>, 
     * or null if this element was not recognized.
     */
    public static QPNode parse(Element e, IDREFResolver idr, 
			       GraphModel gm) {
	String name = e.getNodeName();
	QPNode qpn = null;

	if (!names.containsKey(name))
	    return qpn;
	else {
	    Class c = (Class) names.get(name);
	    try {
		// Create an instance of the appropriate class
		qpn = (QPNode) c.newInstance();
		
		// Pass the Element to the instance for further parsing
		qpn.parse(e, idr, gm);
	    }
	    catch (Exception exc) {
		System.out.println("An appropriate instance of " + 
				   c.getName() + " could not be created");
		exc.printStackTrace(); // XXX
		return null;
	    }
	}
	return qpn;
    }

    public static QPNode getNode(String nodeType) {
	QPNode qpn = null;

	if (!names.containsKey(nodeType))
	    return qpn;
	else {
	    Class c = (Class) names.get(nodeType);
	    try {
		// Create an instance of the appropriate class
		qpn = (QPNode) c.newInstance();
	    }
	    catch (Exception exc) {
		System.out.println("An appropriate instance of " + 
				   c.getName() + " could not be created");
		exc.printStackTrace(); // XXX
		return null;
	    }
	}
	return qpn;
    }
}

