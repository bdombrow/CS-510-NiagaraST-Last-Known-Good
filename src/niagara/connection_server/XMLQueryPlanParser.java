/**
 * $Id: XMLQueryPlanParser.java,v 1.40 2003/03/03 08:23:13 tufte Exp $
 * Generate a physical plan from an XML Description
 *
 */
package niagara.connection_server;

import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;
import java.util.*;
import gnu.regexp.*;

import niagara.xmlql_parser.op_tree.*;
import niagara.logical.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.query_engine.MTException;
import niagara.utils.*;

import niagara.logical.*;
import niagara.ndom.*;
import niagara.optimizer.Plan;
import niagara.optimizer.colombia.*;
import niagara.firehose.*;

public class XMLQueryPlanParser {
    // Maps IDs to elements
    Hashtable ids2els;

    // Maps IDs to logical expressions
    Hashtable ids2plans;

    // Maps IDs to logical properties
    Hashtable ids2logprops;

    // Roots of remote plans
    ArrayList remotePlans;

    Catalog catalog;

    public void initialize() {
        ids2els = new Hashtable();
        ids2plans = new Hashtable();
        ids2logprops = new Hashtable();
        catalog = NiagraServer.getCatalog();
    }

    public void clear() {
        ids2els.clear();
        ids2plans.clear();
        ids2logprops.clear();
    }

    public XMLQueryPlanParser() {
        initialize();
    }

    public Plan parse(String description) throws InvalidPlanException {
        description = description.trim();
        this.remotePlans = new ArrayList();

        niagara.ndom.DOMParser p;
        Document document = null;
        try {
            p = DOMFactory.newParser();
            p.parse(
                new InputSource(
                    new ByteArrayInputStream(description.getBytes())));
            document = p.getDocument();
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
        if (nodeName.equals("unnest")) {
            handleUnnest(e);
        } else if (nodeName.equals("select")) {
            handleSelect(e);
        } else if (nodeName.equals("sort")) {
            handleSort(e);
        } else if (nodeName.equals("join")) {
            handleJoin(e, inputs);
        } else if (nodeName.equals("avg")) {
            handleAvg(e);
        } else if (nodeName.equals("slidingAvg")) {
            handleSlidingAvg(e);
        } else if (nodeName.equals("sum")) {
            handleSum(e);
        } else if (nodeName.equals("slidingSum")) {
            handleSlidingSum(e);
        } else if (nodeName.equals("max")) {
            handleMax(e);
        } else if (nodeName.equals("slidingMax")) {
            handleSlidingMax(e);
        } else if (nodeName.equals("count")) {
            handleCount(e);
        } else if (nodeName.equals("slidingCount")) {
            handleSlidingCount(e);
        } else if (nodeName.equals("dtdscan")) {
            handleDtdScan(e);
        } else if (nodeName.equals("constant")) {
            handleConstant(e);
        } else if (nodeName.equals("construct")) {
            handleConstruct(e);
        } else if (nodeName.equals("firehosescan")) {
            handleFirehoseScan(e);
        } else if (nodeName.equals("filescan")) {
            handleFileScan(e);
        } else if (nodeName.equals("accumulate")) {
            handleAccumulate(e);
        } else if (nodeName.equals("expression")) {
            handleExpression(e);
        } else if (nodeName.equals("dup")) {
            handleDup(e);
        } else if (nodeName.equals("union")) {
            handleUnion(e, inputs);
        } else if (nodeName.equals("send")) {
            handleSend(e);
        } else if (nodeName.equals("display")) {
            handleDisplay(e);
        } else if (nodeName.equals("resource")) {
            handleResource(e);
        } else if (nodeName.equals("incrmax")) {
            handleIncrMax(e);
        } else if (nodeName.equals("incravg")) {
            handleIncrAvg(e);
        } else {
            Class opClass = catalog.getOperatorClass(nodeName);
            if (opClass == null)
            throw new InvalidPlanException(
                            "Unknown operator: " + nodeName);            
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
            int arity = operator.getArity();
            LogicalProperty[] inputProperties = new LogicalProperty[arity];
            for (int i = 0; i < arity; i++)
                inputProperties[i]  = (LogicalProperty) ids2logprops.get(inputs.get(i));
            operator.loadFromXML(e, inputProperties); 
            Plan[] inputPlans = new Plan[arity];
            for (int i = 0; i < arity; i++)
                inputPlans[i] = (Plan) ids2plans.get(inputs.get(i));
            ids2plans.put(nodeName, new Plan(operator, inputPlans));
            ids2logprops.put(nodeName, operator.findLogProp(catalog, inputProperties));
        }

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

    void handleUnnest(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        String inputAttr = e.getAttribute("input");
        String typeAttr = e.getAttribute("type");
        String rootAttr = e.getAttribute("root");
        String regexpAttr = e.getAttribute("regexp");

        int type;
        if (typeAttr.equals("tag")) {
            type = varType.TAG_VAR;
        } else if (typeAttr.equals("element")) {
            type = varType.ELEMENT_VAR;
        } else { // (typeAttr.equals("content"))
            type = varType.CONTENT_VAR;
        }

        Attribute unnestedVariable = new Variable(id, type);

        Scanner scanner;
        regExp redn = null;
        try {
            scanner = new Scanner(new StringReader(regexpAttr));
            REParser rep = new REParser(scanner);
            redn = (regExp) rep.parse().value;
            rep.done_parsing();
        } catch (Exception ex) { // ugh cup throws "Exception!!!"
            ex.printStackTrace();
            throw new InvalidPlanException(
                "Error while parsing: " + regexpAttr + " in " + id);
        }

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        Attribute root;
        if (rootAttr.length() > 0) {
            root = findVariable(inputLogProp, rootAttr);
        } else {
            // If root attr is left blank, we start the regexp from the last
            // attribute added to the input tuple
            Attrs attrs = inputLogProp.getAttrs();
            root = attrs.get(attrs.size() - 1);
        }

        Unnest op = new Unnest(unnestedVariable, root, redn, null);

        Plan scanNode = new Plan(op, input);

        ids2plans.put(id, scanNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleExpression(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        String inputAttr = e.getAttribute("input");
        String classAttr = e.getAttribute("class");
        String expressionAttr = e.getAttribute("expression");
        String variablesAttr = e.getAttribute("variables");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        Class expressionClass = null;
        String expression = null;

        if (!classAttr.equals("")) {
            try {
                expressionClass = Class.forName(classAttr);
            } catch (ClassNotFoundException cnfe) {
                throw new InvalidPlanException(
                    "Class " + classAttr + " could not be found");
            }
        } else if (!expressionAttr.equals("")) {
            expression = expressionAttr;
        } else
            throw new InvalidPlanException("Either a class, or an expression to be interpreted must be defined for an expression operator");

        Attrs variablesUsed = new Attrs();
        if (variablesAttr.equals("*")) {
            variablesUsed = inputLogProp.getAttrs().copy();
        } else {
            StringTokenizer st = new StringTokenizer(variablesAttr);
            while (st.hasMoreTokens()) {
                String varName = st.nextToken();
                Attribute attr = inputLogProp.getAttr(getVarName(varName));
                if (attr == null)
                    throw new InvalidPlanException(
                        "Unknown variable: " + varName);
                variablesUsed.add(attr);
            }
        }

        ExpressionOp op =
            new ExpressionOp(id, variablesUsed, expressionClass, expression);

        Plan expressionNode = new Plan(op, input);

        ids2plans.put(id, expressionNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSelect(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        String inputAttr = e.getAttribute("input");

        NodeList children = e.getChildNodes();
        Element predElt = null;
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                predElt = (Element) children.item(i);
                break;
            }
        }

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        selectOp op = new selectOp(parsePreds(predElt, inputLogProp, null));

        Plan selectNode = new Plan(op, input);
        ids2plans.put(id, selectNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleUnion(Element e, Vector inputs) throws InvalidPlanException {
        String id = e.getAttribute("id");

        UnionOp op = new UnionOp();
        op.setArity(inputs.size());

        Plan[] input_arr = new Plan[inputs.size()];
        LogicalProperty[] inputLogProps = new LogicalProperty[inputs.size()];
        for (int i = 0; i < input_arr.length; i++) {
            input_arr[i] = (Plan) ids2plans.get(inputs.get(i));
            inputLogProps[i] =
                (LogicalProperty) ids2logprops.get(inputs.get(i));
            if (inputLogProps[i].getDegree() != inputLogProps[0].getDegree())
                throw new InvalidPlanException("Union inputs are not union-compatible");
        }

        Plan node = new Plan(op, input_arr);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, inputLogProps));
    }

    void handleDup(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        dupOp op = new dupOp();

        String inputAttr = e.getAttribute("input");
        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        String branchAttr = e.getAttribute("branch");
        // XXX vpapad: catch format exception, check that we really have
        // that many output streams - why do we have to specify this here?
        int branch = Integer.parseInt(branchAttr);
        op.setDup(branch);

        Plan node = new Plan(op, input);
        ids2plans.put(id, node);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSort(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        SortOp op = new SortOp();

        String inputAttr = e.getAttribute("input");
        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        String sortbyAttr = e.getAttribute("sort_by");
        Attribute sortBy = findVariable(inputLogProp, sortbyAttr);

        short comparisonMethod;
        String comparisonAttr = e.getAttribute("comparison");
        if (comparisonAttr.equals("alphabetic"))
            comparisonMethod = SortOp.ALPHABETIC_COMPARISON;
        else
            comparisonMethod = SortOp.NUMERIC_COMPARISON;

        boolean ascending;
        String orderAttr = e.getAttribute("order");
        ascending = !orderAttr.equals("descending");
        op.setSort(sortBy, comparisonMethod, ascending);

        Plan sortNode = new Plan(op, input);
        ids2plans.put(id, sortNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleJoin(Element e, Vector inputs) throws InvalidPlanException {
        String id = e.getAttribute("id");

        joinOp op = new joinOp();
        Plan left = (Plan) ids2plans.get(inputs.get(0));
        LogicalProperty leftLogProp =
            (LogicalProperty) ids2logprops.get(inputs.get(0));

        Plan right = (Plan) ids2plans.get(inputs.get(1));
        LogicalProperty rightLogProp =
            (LogicalProperty) ids2logprops.get(inputs.get(1));

        NodeList children = e.getChildNodes();
        Element predElt = null;

        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                predElt = (Element) children.item(i);
                break;
            }
        }

        Predicate pred = parsePreds(predElt, leftLogProp, rightLogProp);

        // In case of an equijoin we have to parse "left" 
        // and "right" to get additional equality predicates
        String leftattrs = e.getAttribute("left");
        String rightattrs = e.getAttribute("right");

        ArrayList leftVars = new ArrayList();
        ArrayList rightVars = new ArrayList();

        if (leftattrs.length() > 0) {
            try {
                RE re = new RE("(\\$)?[a-zA-Z0-9_]+");
                REMatch[] all_left = re.getAllMatches(leftattrs);
                REMatch[] all_right = re.getAllMatches(rightattrs);
                for (int i = 0; i < all_left.length; i++) {
                    Attribute leftAttr =
                        findVariable(leftLogProp, all_left[i].toString());
                    Attribute rightAttr =
                        findVariable(rightLogProp, all_right[i].toString());
                    leftVars.add(leftAttr);
                    rightVars.add(rightAttr);
                }
            } catch (REException rx) {
                throw new InvalidPlanException(
                    "Syntax error in equijoin predicate specification for "
                        + id);
            }
        }
        String extensionJoinAttr = e.getAttribute("extensionjoin");
	int extJoin;
	if(extensionJoinAttr.equals("right")) {
	    extJoin = joinOp.RIGHT;
	} else if (extensionJoinAttr.equals("left")) {
	    extJoin = joinOp.LEFT;
	} else if (extensionJoinAttr.equals("none")) {
	    extJoin = joinOp.NONE;
	} else if (extensionJoinAttr.equals("both")) {
	    extJoin = joinOp.BOTH;
	} else {
	    throw new InvalidPlanException("Invalid extension join value " +
					   extensionJoinAttr);
	}

        op.setJoin(pred, leftVars, rightVars, extJoin);

        Plan joinNode = new Plan(op, left, right);
        ids2plans.put(id, joinNode);
        ids2logprops.put(
            id,
            op.findLogProp(
                catalog,
                new LogicalProperty[] { leftLogProp, rightLogProp }));
    }

    void handleAvg(Element e) throws InvalidPlanException {
        averageOp op = new averageOp();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String avgattr = e.getAttribute("avgattr");
        String inputAttr = e.getAttribute("input");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute averagingAttribute = findVariable(inputLogProp, avgattr);
        op.setAverageInfo(new skolem(id, groupbyAttrs), averagingAttribute);

        Plan avgNode = new Plan(op, input);
        ids2plans.put(id, avgNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSlidingAvg(Element e) throws InvalidPlanException {
        SlidingAverageOp op = new SlidingAverageOp();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String avgattr = e.getAttribute("avgattr");
        String inputAttr = e.getAttribute("input");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        //
        Integer rangeValue;
        Integer everyValue;
        if (range != "") {
            rangeValue = new Integer(range);
            if (rangeValue.intValue() <= 0)
                throw new InvalidPlanException("range must greater than zero");
        } else
            throw new InvalidPlanException("range ???");
        if (every != "") {
            everyValue = new Integer(every);
            if (everyValue.intValue() <= 0)
                throw new InvalidPlanException("every must greater than zero");
        } else
            throw new InvalidPlanException("every ???");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute averagingAttribute = findVariable(inputLogProp, avgattr);
        op.setAverageInfo(new skolem(id, groupbyAttrs), averagingAttribute);
        op.setWindowInfo(rangeValue.intValue(), everyValue.intValue());

        Plan avgNode = new Plan(op, input);
        ids2plans.put(id, avgNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSum(Element e) throws InvalidPlanException {
        SumOp op = new SumOp();

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String sumattr = e.getAttribute("sumattr");
        String inputAttr = e.getAttribute("input");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute summingAttribute = findVariable(inputLogProp, sumattr);
        op.setSummingInfo(new skolem(id, groupbyAttrs), summingAttribute);

        Plan sumNode = new Plan(op, input);
        ids2plans.put(id, sumNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSlidingSum(Element e) throws InvalidPlanException {
        SlidingSumOp op = new SlidingSumOp();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String sumattr = e.getAttribute("sumattr");
        String inputAttr = e.getAttribute("input");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        //
        Integer rangeValue;
        Integer everyValue;
        if (range != "") {
            rangeValue = new Integer(range);
            if (rangeValue.intValue() <= 0)
                throw new InvalidPlanException("range must greater than zero");
        } else
            throw new InvalidPlanException("range ???");
        if (every != "") {
            everyValue = new Integer(every);
            if (everyValue.intValue() <= 0)
                throw new InvalidPlanException("every must greater than zero");
        } else
            throw new InvalidPlanException("every ???");

        op.setWindowInfo(rangeValue.intValue(), everyValue.intValue());

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute summingAttribute = findVariable(inputLogProp, sumattr);
        op.setSummingInfo(new skolem(id, groupbyAttrs), summingAttribute);

        Plan sumNode = new Plan(op, input);
        ids2plans.put(id, sumNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleMax(Element e) throws InvalidPlanException {
        MaxOp op = new MaxOp();

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String maxattr = e.getAttribute("maxattr");
        String inputAttr = e.getAttribute("input");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute maxingAttribute = findVariable(inputLogProp, maxattr);
        op.setMaxingInfo(new skolem(id, groupbyAttrs), maxingAttribute);

        Plan maxNode = new Plan(op, input);
        ids2plans.put(id, maxNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSlidingMax(Element e) throws InvalidPlanException {
        SlidingMaxOp op = new SlidingMaxOp();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String maxattr = e.getAttribute("maxattr");
        String inputAttr = e.getAttribute("input");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        //
        Integer rangeValue;
        Integer everyValue;
        if (range != "") {
            rangeValue = new Integer(range);
            if (rangeValue.intValue() <= 0)
                throw new InvalidPlanException("range must greater than zero");
        } else
            throw new InvalidPlanException("range ???");
        if (every != "") {
            everyValue = new Integer(every);
            if (everyValue.intValue() <= 0)
                throw new InvalidPlanException("every must greater than zero");
        } else
            throw new InvalidPlanException("every ???");

        op.setWindowInfo(rangeValue.intValue(), everyValue.intValue());

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute maxingAttribute = findVariable(inputLogProp, maxattr);
        op.setMaxingInfo(new skolem(id, groupbyAttrs), maxingAttribute);

        Plan maxNode = new Plan(op, input);
        ids2plans.put(id, maxNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleCount(Element e) throws InvalidPlanException {
        CountOp op = new CountOp();

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String countattr = e.getAttribute("countattr");
        String inputAttr = e.getAttribute("input");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute countingAttribute = findVariable(inputLogProp, countattr);
        op.setCountingInfo(new skolem(id, groupbyAttrs), countingAttribute);

        Plan countNode = new Plan(op, input);
        ids2plans.put(id, countNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleSlidingCount(Element e) throws InvalidPlanException {
        SlidingCountOp op = new SlidingCountOp();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String countattr = e.getAttribute("countattr");
        String inputAttr = e.getAttribute("input");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        //
        Integer rangeValue;
        Integer everyValue;
        if (range != "") {
            rangeValue = new Integer(range);
            if (rangeValue.intValue() <= 0)
                throw new InvalidPlanException("range must greater than zero");
        } else
            throw new InvalidPlanException("range ???");
        if (every != "") {
            everyValue = new Integer(every);
            if (everyValue.intValue() <= 0)
                throw new InvalidPlanException("every must greater than zero");
        } else
            throw new InvalidPlanException("every ???");

        op.setWindowInfo(rangeValue.intValue(), everyValue.intValue());

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute countingAttribute = findVariable(inputLogProp, countattr);
        op.setCountingInfo(new skolem(id, groupbyAttrs), countingAttribute);

        Plan countNode = new Plan(op, input);
        ids2plans.put(id, countNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleDtdScan(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        // The node's children contain URLs
        Vector urls = new Vector();
        NodeList children = ((Element) e).getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            urls.addElement(((Element) child).getAttribute("value"));
        }

        dtdScanOp op = new dtdScanOp();
        op.setDocs(urls);

        op.setVariable(new Variable(id, NodeDomain.getDOMNode()));

        Plan node = new Plan(op);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, new LogicalProperty[] {
        }));
    }

    void handleResource(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        String urn = e.getAttribute("urn");

        ResourceOp op =
            new ResourceOp(
                new Variable(id, NodeDomain.getDOMNode()),
                urn,
                catalog);

        Plan node = new Plan(op, false);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, new LogicalProperty[] {
        }));
    }

    void handleConstant(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        String content = "";
        NodeList children = e.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            content = content + XMLUtils.flatten(children.item(i), false);
        }

        String vars = e.getAttribute("vars");
        // Parse the vars attribute 
        // XXX vpapad: does this work?
        StringTokenizer st = new StringTokenizer(vars, ",");
        ArrayList variables = new ArrayList();
        while (st.hasMoreTokens()) {
            variables.add(new Variable(st.nextToken()));
        }

        if (variables.size() == 0) {
            variables.add(new Variable(id));
        }

        ConstantOp op = new ConstantOp();
        op.setContent(content);
        op.setVars(variables);

        Plan node = new Plan(op);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, new LogicalProperty[] {
        }));
    }

    void handleSend(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        SendOp op = new SendOp();

        op.setSelectedAlgoIndex(0);

        String inputAttr = e.getAttribute("input");
        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        Plan node = new Plan(op, input);
        ids2plans.put(id, node);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleDisplay(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        DisplayOp op = new DisplayOp();

        op.setQueryId(e.getAttribute("query_id"));
        op.setClientLocation(e.getAttribute("client_location"));

        String inputAttr = e.getAttribute("input");
        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        Plan node = new Plan(op, input);
        ids2plans.put(id, node);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleFirehoseScan(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        String host = e.getAttribute("host");
        int port = Integer.parseInt(e.getAttribute("port"));
        int rate = Integer.parseInt(e.getAttribute("rate"));
        String dataTypeStr = e.getAttribute("datatype");
        String descriptor = e.getAttribute("desc");
        String descriptor2 = e.getAttribute("desc2");

        int numGenCalls = Integer.parseInt(e.getAttribute("num_gen_calls"));
        int numTLElts = Integer.parseInt(e.getAttribute("num_tl_elts"));
        boolean prettyPrint =
            e.getAttribute("prettyprint").equalsIgnoreCase("yes");
        String trace = e.getAttribute("trace");

        int dataType = -1;
        boolean found = false;
        for (int i = 0; i < FirehoseConstants.numDataTypes; i++) {
            if (dataTypeStr.equalsIgnoreCase(FirehoseConstants.typeNames[i])) {
                dataType = i;
                found = true;
                break;
            }
        }
        if (found == false)
            throw new InvalidPlanException(
                "Invalid type - typeStr: " + dataTypeStr);

        boolean useStreamFormat = NiagraServer.usingSAXDOM();

        FirehoseSpec fhSpec =
            new FirehoseSpec(
                port,
                host,
                dataType,
                descriptor,
                descriptor2,
                numGenCalls,
                numTLElts,
                rate,
                useStreamFormat,
                prettyPrint,
                trace);

        FirehoseScanOp op = new FirehoseScanOp();
        op.setFirehoseScan(fhSpec, new Variable(id));

        Plan node = new Plan(op);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, new LogicalProperty[] {
        }));
    }

    void handleFileScan(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");
        boolean isStream = e.getAttribute("isstream").equalsIgnoreCase("yes");
        FileScanSpec fsSpec =
            new FileScanSpec(e.getAttribute("filename"), isStream);

        FileScanOp op = new FileScanOp();
        op.setFileScan(fsSpec, new Variable(id));

        Plan node = new Plan(op);
        ids2plans.put(id, node);
        ids2logprops.put(id, op.findLogProp(catalog, new LogicalProperty[] {
        }));
    }

    void handleConstruct(Element e) throws InvalidPlanException {
        String id = e.getAttribute("id");

        constructOp op = new constructOp();
        NodeList children = e.getChildNodes();
        String content = "";
        for (int i = 0; i < children.getLength(); i++) {
            int nodeType = children.item(i).getNodeType();
            if (nodeType == Node.ELEMENT_NODE)
                content += XMLUtils.explosiveFlatten(children.item(i));
            else if (nodeType == Node.CDATA_SECTION_NODE)
                content += children.item(i).getNodeValue();
        }
        String inputAttr = e.getAttribute("input");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        Scanner scanner;
        constructBaseNode cn = null;

        try {
            scanner = new Scanner(new StringReader(content));
            ConstructParser cep = new ConstructParser(scanner);
            cn = (constructBaseNode) cep.parse().value;
            cep.done_parsing();
        } catch (Exception ex) {
            throw new InvalidPlanException("Error while parsing: " + content);
        }

        op.setVariable(new Variable(id));
        op.setConstruct(cn);
        Plan node = new Plan(op, (Plan) ids2plans.get(inputAttr));
        ids2plans.put(id, node);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    private void handleAccumulate(Element e) throws InvalidPlanException {
        try {
            /* Need to create an AccumulateOp
             * 1) The MergeTemplate
             * 2) The MergeIndex - index of attribute to work on
             */
            String id = e.getAttribute("id");

            /* Either a file name, URI or Merge template string */
            String mergeTemplate = e.getAttribute("mergeTemplate");

            /* input specifies input operator, index specifies index
             * of attribute to work on 
             */
            String inputAttr = e.getAttribute("input");
            String mergeAttr = e.getAttribute("mergeAttr");

            /* name by which the accumulated file should be referred to */
            String accumFileName = e.getAttribute("accumFileName");

            /* file containing the initial input to the accumulate */
            String initialAccumFile = e.getAttribute("initialAccumFile");

            AccumulateOp op = new AccumulateOp();

            String clear = e.getAttribute("clear");
            boolean cl = (!clear.equals("false"));

            Plan inputNode = (Plan) ids2plans.get(inputAttr);
            LogicalProperty inputLogProp =
                (LogicalProperty) ids2logprops.get(inputAttr);

            Attribute mergeAttribute = findVariable(inputLogProp, mergeAttr);

            op.setAccumulate(
                mergeTemplate,
                mergeAttribute,
                accumFileName,
                initialAccumFile,
                cl);

            Plan accumNode = new Plan(op, inputNode);
            ids2plans.put(id, accumNode);
            ids2logprops.put(
                id,
                op.findLogProp(
                    catalog,
                    new LogicalProperty[] { inputLogProp }));
        } catch (MTException mte) {
            throw new InvalidPlanException(
                "Invalid Merge Template" + mte.getMessage());
        }
    }

    void handleIncrMax(Element e) throws InvalidPlanException {
        IncrementalMax op = new IncrementalMax();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String maxattr = e.getAttribute("maxattr");
        String inputAttr = e.getAttribute("input");
        String emptyGroupValueAttr = e.getAttribute("emptygroupvalue");

        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute maxAttribute = findVariable(inputLogProp, maxattr);

        op.setSkolemAttributes(new skolem(id, groupbyAttrs));
        op.setMaxAttribute(maxAttribute);
        op.setEmptyGroupValue(Double.valueOf(emptyGroupValueAttr));

        Plan maxNode = new Plan(op, input);
        ids2plans.put(id, maxNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    void handleIncrAvg(Element e) throws InvalidPlanException {
        IncrementalAverage op = new IncrementalAverage();
        op.setSelectedAlgoIndex(0);

        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String avgattr = e.getAttribute("avgattr");
        String inputAttr = e.getAttribute("input");
        Plan input = (Plan) ids2plans.get(inputAttr);
        LogicalProperty inputLogProp =
            (LogicalProperty) ids2logprops.get(inputAttr);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute avgAttribute = findVariable(inputLogProp, avgattr);

        op.setSkolemAttributes(new skolem(id, groupbyAttrs));
        op.setAvgAttribute(avgAttribute);

        Plan maxNode = new Plan(op, input);
        ids2plans.put(id, maxNode);
        ids2logprops.put(
            id,
            op.findLogProp(catalog, new LogicalProperty[] { inputLogProp }));
    }

    // XXX This code should be rewritten to take advantage of 
    // XXX getName and getCode in opType
    Predicate parsePreds(
        Element e,
        LogicalProperty leftv,
        LogicalProperty rightv)
        throws InvalidPlanException {
        if (e == null)
            return True.getTrue();

        Element l, r;
        l = r = null;

        Node c = e.getFirstChild();
        do {
            if (c.getNodeType() == Node.ELEMENT_NODE) {
                if (l == null)
                    l = (Element) c;
                else if (r == null)
                    r = (Element) c;
            }
            c = c.getNextSibling();
        } while (c != null);

        if (e.getNodeName().equals("and")) {
            Predicate left = parsePreds(l, leftv, rightv);
            Predicate right = parsePreds(r, leftv, rightv);

            return new And(left, right);
        } else if (e.getNodeName().equals("or")) {
            Predicate left = parsePreds(l, leftv, rightv);
            Predicate right = parsePreds(r, leftv, rightv);

            return new Or(left, right);
        } else if (e.getNodeName().equals("not")) {
            Predicate child = parsePreds(l, leftv, rightv);

            return new Not(child);
        } else { // Relational operator
            Atom left = parseAtom(l, leftv, rightv);
            Atom right = parseAtom(r, leftv, rightv);

            int type;
            String op = e.getAttribute("op");

            if (op.equals("lt"))
                type = opType.LT;
            else if (op.equals("gt"))
                type = opType.GT;
            else if (op.equals("le"))
                type = opType.LEQ;
            else if (op.equals("ge"))
                type = opType.GEQ;
            else if (op.equals("ne"))
                type = opType.NEQ;
            else if (op.equals("eq"))
                type = opType.EQ;
            else if (op.equals("contain"))
                type = opType.CONTAIN;
            else
                throw new InvalidPlanException(
                    "Unrecognized predicate op: " + op);

            return Comparison.newComparison(type, left, right);
            // XXX vpapad: removed various toVarList @#$@#,
            // supposed to help in toXML. Test it!
        }
    }

    Atom parseAtom(Element e, LogicalProperty left, LogicalProperty right)
        throws InvalidPlanException {
        if (e.getNodeName().equals("number"))
            return new NumericConstant(e.getAttribute("value"));
        else if (e.getNodeName().equals("string"))
            return new StringConstant(e.getAttribute("value"));
        else { //var 
            String varname = e.getAttribute("value");
            // chop initial $ sign off
            if (varname.charAt(0) == '$')
                varname = varname.substring(1);
            Variable v = (Variable) left.getAttr(varname);
            if (v == null)
                v = (Variable) right.getAttr(varname);
            if (v != null)
                return v;
            else
                throw new InvalidPlanException(
                    "Unknown variable name: " + varname);
        }
    }

    public ArrayList getRemotePlans() {
        return remotePlans;
    }

    public Attribute findVariable(LogicalProperty logProp, String varName)
        throws InvalidPlanException {
        Attribute attr = logProp.getAttr(getVarName(varName));
        if (attr == null)
            throw new InvalidPlanException("Unknown variable: " + varName);
        return attr;
    }

    /** Strip dollar signs from varName */
    public String getVarName(String varName) {
        if (varName.charAt(0) == '$')
            return varName.substring(1);
        return varName;
    }
}
