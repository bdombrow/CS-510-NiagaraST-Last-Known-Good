/**
 * $Id: XMLQueryPlanParser.java,v 1.47 2003/09/26 21:25:13 vpapad Exp $
 * Generate a physical plan from an XML Description
 *
 */
package niagara.connection_server;

import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;
import java.util.*;

import niagara.xmlql_parser.op_tree.*;
import niagara.logical.*;

import niagara.ndom.*;
import niagara.optimizer.Plan;
import niagara.optimizer.colombia.*;

public class XMLQueryPlanParser {
    // Maps IDs to elements
    private Hashtable ids2els;

    // Maps IDs to logical expressions
    private Hashtable ids2plans;

    // Maps IDs to logical properties
    private Hashtable ids2logprops;

    // Roots of remote plans
    private ArrayList remotePlans;

    private niagara.ndom.DOMParser parser;
    
    // The server catalog
    private Catalog catalog;

    public XMLQueryPlanParser() {
        ids2els = new Hashtable();
        ids2plans = new Hashtable();
        ids2logprops = new Hashtable();
        catalog = NiagraServer.getCatalog();
        parser = DOMFactory.newValidatingParser(); 
    }

    public void clear() {
        ids2els.clear();
        ids2plans.clear();
        ids2logprops.clear();
    }

    public Plan parse(String description) throws InvalidPlanException {
        description = description.trim();
        this.remotePlans = new ArrayList();

        Document document = null;
        try {
            parser.parse(
                new InputSource(
                    new ByteArrayInputStream(description.getBytes())));
            if (parser.hasErrors() || parser.hasWarnings()) {
                String msg = parser.getErrorStrings() + parser.getWarningStrings();
                throw new InvalidPlanException(
                    "Error parsing plan string:" + msg);
            }
            document = parser.getDocument();
            Element root = (Element) document.getDocumentElement();
            return parsePlan(root);
        } catch (org.xml.sax.SAXException se) {
            throw new InvalidPlanException(
                "Error parsing plan string " + se.getMessage());
        } catch (java.io.IOException ioe) {
            throw new InvalidPlanException(
                "IO Exception reading plan string " + ioe.getMessage());
        }
    }

    Plan parsePlan(Element root) throws InvalidPlanException {
        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() != Node.ELEMENT_NODE)
                continue;
            Element e = (Element) children.item(i);
            String id = e.getAttribute("id");
            if (id.length() == 0)
                throw new InvalidPlanException("Each operator must have an id attribute");
            if (ids2els.get(id) != null)
                throw new InvalidPlanException(
                    "Duplicate operator id found: " + id);
            ids2els.put(e.getAttribute("id"), e);
        }
        String top = root.getAttribute("top");
        Element e = (Element) ids2els.get(top);
        if (e == null) {
            throw new InvalidPlanException("Invalid top node");
        }
        parseOperator(e);

        Plan p = (Plan) ids2plans.get(top);
        // Put an appropriate project in front of the user's top operator,
        // projecting on the last output attribute for the top operator
        Attrs outputSchema =
            ((LogicalProperty) ids2logprops.get(top)).getAttrs();
        if (outputSchema.size() != 0)
            outputSchema = new Attrs(outputSchema.get(outputSchema.size() - 1));
        return new Plan(new Project(outputSchema), p);
    }

    /**
     * @param e an <code>Element</code> describing a logical plan operator
     * @exception InvalidPlanException the description of the plan was invalid
     */
    void parseOperator(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        // If we already visited this node, just return 
        if (ids2plans.containsKey(id))
            return;

        String nodeName = e.getNodeName();

        // visit all the node's inputs
        String inputsAttr = e.getAttribute("input");
        Vector inputs = new Vector();
        StringTokenizer st = new StringTokenizer(inputsAttr);
        while (st.hasMoreTokens())
            inputs.addElement(st.nextToken());

        // XXX vpapad: Check that the operator only uses variables
        // defined in its inputs 
        for (int i = 0; i < inputs.size(); i++) {
            Element el = (Element) ids2els.get(inputs.get(i));
            if (el == null)
                throw new InvalidPlanException(
                    "Could not find input "
                        + inputs.get(i)
                        + " of operator "
                        + id);
            parseOperator(el);
        }

        // Now that all inputs are handled 
        // create a Plan for this operator
        Class opClass = catalog.getOperatorClass(nodeName);
        if (opClass == null)
            throw new InvalidPlanException("Unknown operator: " + nodeName);
        op operator;
        try {
            operator = (op) opClass.newInstance();
        } catch (ClassCastException cce) {
            throw new InvalidPlanException(
                nodeName + " is not a logical operator");
        } catch (InstantiationException ie) {
            throw new InvalidPlanException(
                "Could not instantiate an "
                    + opClass
                    + " object for "
                    + nodeName);
        } catch (IllegalAccessException iae) {
            throw new InvalidPlanException(
                "Illegal access exception while instantiating "
                    + opClass
                    + " for "
                    + nodeName);
        }
        int arity = inputs.size();
        LogicalProperty[] inputProperties = new LogicalProperty[arity];
        for (int i = 0; i < arity; i++)
            inputProperties[i] =
                (LogicalProperty) ids2logprops.get(inputs.get(i));
        operator.loadFromXML(e, inputProperties);
	operator.setId(id); // KT - for debugging/profiling output
        Plan[] inputPlans = new Plan[arity];
        for (int i = 0; i < arity; i++)
            inputPlans[i] = (Plan) ids2plans.get(inputs.get(i));
        ids2plans.put(id, new Plan(operator, inputPlans));
        ids2logprops.put(id, operator.findLogProp(catalog, inputProperties));

        String location = e.getAttribute("location");
        // Is this operator supposed to run at a remote site
        if (location.length() > 0) {
            // XXX vpapad: This code will break unless:
            //  1. Specified locations are never our own
            //  2. Remote subplans are never nested 
            Plan remoteExpr = (Plan) ids2plans.get(id);
            SendOp sop = new SendOp(location);
            remotePlans.add(new Plan(sop, remoteExpr));

            // Put a receive operator in the place of the remote plan
            ReceiveOp rop = new ReceiveOp(sop);
            rop.setLogProp((LogicalProperty) ids2logprops.get(id));
            ids2plans.put(id, new Plan(rop));
        }
    }

    public ArrayList getRemotePlans() {
        return remotePlans;
    }
}
