/**
 * $Id: XMLQueryPlanParser.java,v 1.28 2002/09/21 10:14:29 vpapad Exp $
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

import niagara.ndom.*;
import niagara.firehose.*;

public class XMLQueryPlanParser {

    // Maps IDs to elements
    Hashtable ids2els;

    // Maps IDs to logical plan nodes
    Hashtable ids2nodes;

    public void initialize() {
	ids2els = new Hashtable();
	ids2nodes = new Hashtable();
    }

    public XMLQueryPlanParser() {
	initialize();
    }

    public logNode parse(String description) throws InvalidPlanException {
        description = description.trim();

	niagara.ndom.DOMParser p;
	Document document = null;
	try {
	    p = DOMFactory.newParser();
	    p.parse(new InputSource(new ByteArrayInputStream(description.getBytes())));
	    document = p.getDocument();
	    Element root = (Element) document.getDocumentElement();
	    return parsePlan(root);
	} catch (org.xml.sax.SAXException se) {
	    throw new InvalidPlanException("Error parsing plan string " 
                                           + se.getMessage());
	} catch (java.io.IOException ioe) {
	    throw new InvalidPlanException("IO Exception reading plan string " 
                                           + ioe.getMessage());
        }
    }

    logNode parsePlan(Element root) throws InvalidPlanException {
	NodeList children = root.getChildNodes();
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i).getNodeType() != Node.ELEMENT_NODE)
		continue;
	    Element e = (Element) children.item(i);
	    ids2els.put(e.getAttribute("id"), e);
	}

	String top = root.getAttribute("top");
	Element e = (Element) ids2els.get(top);
	if(e == null) {
	    throw new InvalidPlanException("Invalid top node");
	}
	parseOperator(e);

        // Assign unique names to all nodes
        // Transfer location attributes
        Enumeration ids = ids2nodes.keys();
        while (ids.hasMoreElements()) {
            String id = (String) ids.nextElement();
            logNode node = (logNode) ids2nodes.get(id);
            node.setName(id);

            Element n = (Element) ids2els.get(id);
            String location = n.getAttribute("location");
            if (location != null && location.length() > 0)
                node.setLocation(location);
        }

	return (logNode) ids2nodes.get(top);
    }

    /**
     * @param e an <code>Element</code> describing a logical plan operator
     * @exception InvalidPlanException the description of the plan was invalid
     */
    void parseOperator(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id"); // VPAPAD/KT hangs here if id does not match input

	// If we already visited this node, just return 
	if (ids2nodes.containsKey(id)) 
	    return;
	String nodeName = e.getNodeName();

	// visit all the node's inputs
	String inputsAttr = e.getAttribute("input");
	Vector inputs = new Vector();
	StringTokenizer st = new StringTokenizer(inputsAttr);
	while (st.hasMoreTokens()) 
	    inputs.addElement(st.nextToken());

	for (int i=0; i < inputs.size(); i++) {
	    Element el = (Element) ids2els.get(inputs.get(i));
	    parseOperator(el);
	}

	// Now that all inputs are handled 
	// create a logNode for this operator
	if (nodeName.equals("scan")) {
	    handleScan(e);
	}
	else if (nodeName.equals("select")) {
	    handleSelect(e);
	}
	else if (nodeName.equals("sort")) {
	    handleSort(e);
	}
	else if (nodeName.equals("join")) {
	    handleJoin(e, inputs);
	}
	else if (nodeName.equals("avg")) {
	    handleAvg(e);
	}
	else if (nodeName.equals("sum")) {
	    handleSum(e);
	}
	else if (nodeName.equals("count")) {
	    handleCount(e);
	}
	else if (nodeName.equals("dtdscan")) {
	    handleDtdScan(e);
	}
	else if (nodeName.equals("constant")) {
	    handleConstant(e);
	}
	else if (nodeName.equals("construct")) {
	    handleConstruct(e);
	} 
	else if (nodeName.equals("firehosescan")) {
	    handleFirehoseScan(e);
	} 
	else if (nodeName.equals("filescan")) {
	    handleFileScan(e);
	} 
	else if (e.getNodeName().equals("accumulate")) {
	    handleAccumulate(e);
	}
	else if (e.getNodeName().equals("expression")) {
	    handleExpression(e);
	}
	else if (e.getNodeName().equals("dup")) {
	    handleDup(e);
	}
	else if (e.getNodeName().equals("union")) {
	    handleUnion(e, inputs);
	}
	else if (e.getNodeName().equals("send")) {
	    handleSend(e);
	}
	else if (e.getNodeName().equals("display")) {
	    handleDisplay(e);
	}
	else if (e.getNodeName().equals("resource")) {
	    handleResource(e);
	}
	else if (e.getNodeName().equals("incrmax")) {
	    handleIncrMax(e);
	}
	else if (e.getNodeName().equals("incravg")) {
	    handleIncrAvg(e);
	}
        else {
            throw new InvalidPlanException("Unknown operator: " + nodeName);
        }
    }

    void handleScan(Element e) throws InvalidPlanException {
	scanOp op = new scanOp();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");

	String inputAttr = e.getAttribute("input");
	String typeAttr = e.getAttribute("type");
	String rootAttr = e.getAttribute("root");
	String regexpAttr = e.getAttribute("regexp");

        op.setDumpAttributes(typeAttr, rootAttr);

	
	int type;
	if (typeAttr.equals("tag")) {
	    type = varType.TAG_VAR;
	}
	else if (typeAttr.equals("element")) {
	    type = varType.ELEMENT_VAR;
	}
	else { // (typeAttr.equals("content"))
	    type = varType.CONTENT_VAR;
	}
	
	logNode input = (logNode) ids2nodes.get(inputAttr);
	
	// Copy the varTbl and schema from the node downstream, or create 
	// empty ones if they don't exist
	
	varTbl qpVarTbl; Schema sc;
	
	if (input.getVarTbl() != null) {
	    qpVarTbl = new varTbl(input.getVarTbl());
	    sc = new Schema(input.getSchema()); 
	}
	else {
	    qpVarTbl = new varTbl();
	    sc = new Schema();
	    sc.addSchemaUnit(new SchemaUnit(null, -1));
	}
	
	
	Scanner scanner;
	regExp redn = null;
	try {
	    scanner = new Scanner(new StringReader(regexpAttr));
	    REParser rep = new REParser(scanner);
	    redn = (regExp) rep.parse().value;
	    rep.done_parsing();
	} catch (Exception ex) { // ugh cup throws "Exception!!!"
	    ex.printStackTrace();
	    throw new InvalidPlanException("Error while parsing: " 
					   + regexpAttr + " in " + id);
	}
	
	schemaAttribute resultSA = 
	    new schemaAttribute(sc.numAttr(), type);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	// The attribute the regexp starts from
	// If left blank, we start the regexp from the last
	// attribute added to the input tuple
	schemaAttribute sa;
    //try {
	    if (rootAttr.equals(""))
		sa = new schemaAttribute(sc.numAttr() - 1);
	    else
		sa = new schemaAttribute(qpVarTbl.lookUp(rootAttr));
    //}
    //catch (Exception exc) {
    //    throw new InvalidPlanException("Parse error: Unknown variable " + rootAttr + " in " + id);

    //	}

	op.setScan(sa, redn);

	sc.addSchemaUnit(new SchemaUnit(redn, sc.numAttr()));

	
	logNode scanNode = new logNode(op, input);
	
	ids2nodes.put(id, scanNode);
	
	scanNode.setSchema(sc);
	scanNode.setVarTbl(qpVarTbl);
    }

    void handleExpression(Element e) throws InvalidPlanException {
	ExpressionOp op = new ExpressionOp();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");

	String inputAttr = e.getAttribute("input");
	String classAttr = e.getAttribute("class");
	String expressionAttr = e.getAttribute("expression");

	logNode input = (logNode) ids2nodes.get(inputAttr);
	
	// Copy the varTbl and schema from the node downstream, or create 
	// empty ones if they don't exist
	
	varTbl qpVarTbl; Schema sc;
	
	if (input.getVarTbl() != null) {
	    qpVarTbl = new varTbl(input.getVarTbl());
	    sc = new Schema(input.getSchema()); 
	}
	else {
	    qpVarTbl = new varTbl();
	    sc = new Schema();
	    sc.addSchemaUnit(new SchemaUnit(null, -1));
	}
	
		
	schemaAttribute resultSA = 
	    new schemaAttribute(sc.numAttr(), varType.ELEMENT_VAR);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	op.setVarTable(qpVarTbl.getMapping());

	if (!classAttr.equals("")) {
	    try {
		op.setExpressionClass(Class.forName(classAttr));
	    }
	    catch (ClassNotFoundException cnfe) {
		throw new InvalidPlanException("Class " + classAttr + " could not be found");
	    }
	}
	else if (!expressionAttr.equals("")) {
	    op.setExpression(expressionAttr);
	}
	else 
	    throw new InvalidPlanException("Either a class, or an expression to be interpreted must be defined for an expression operator");

	sc.addSchemaUnit(new SchemaUnit(null, sc.numAttr()));
	logNode expressionNode = new logNode(op, input);
	
	ids2nodes.put(id, expressionNode);
	
	expressionNode.setSchema(sc);
	expressionNode.setVarTbl(qpVarTbl);
    }

    void handleSelect(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

	selectOp op = new selectOp();
	op.setSelectedAlgoIndex(0);
	
	String inputAttr = e.getAttribute("input");
	NodeList children = e.getChildNodes();
	
	Element predElt = null;
	
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i) instanceof Element) {
		predElt = (Element) children.item(i);
		break;
	    }
	}
	
	logNode input =  (logNode) ids2nodes.get(inputAttr);
	


	varTbl qpVarTbl = input.getVarTbl();
	predicate pred = parsePreds(predElt, qpVarTbl, null);
	
	Vector v = new Vector(); v.addElement(pred);
	op.setSelect(v);
	
	logNode selectNode = new logNode(op, input);
	ids2nodes.put(id, selectNode);

        // XXX Hack to null out variables 
        // XXX that we know won't be used downstream
        // XXX This process should be automated
        String clearAttr = e.getAttribute("clear");
	boolean[] clear = new boolean[qpVarTbl.size()];
	StringTokenizer st = new StringTokenizer(clearAttr);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    int varPos = qpVarTbl.lookUp(varName).getPos();
	    clear[varPos] = true;
	}
        op.setClear(clear);
        op.setClearAttr(clearAttr);
	
	// Nothing changes for the variables
	// Just use whatever variable table the node downstream has
	selectNode.setSchema(input.getSchema());
	selectNode.setVarTbl(qpVarTbl);
    }

    void handleUnion(Element e, Vector inputs) throws InvalidPlanException {
	String id = e.getAttribute("id");

	UnionOp op = new UnionOp();
	op.setSelectedAlgoIndex(0);
	
	String inputAttr = e.getAttribute("input");
	
	logNode[] input_arr = new logNode[inputs.size()];
	for (int i=0; i < input_arr.length; i++) {
	    input_arr[i] = (logNode) ids2nodes.get(inputs.get(i));
	}

	logNode node = new logNode(op, input_arr);
	ids2nodes.put(id, node);

	// Just keep the schema and variable table of the first input
	logNode first_input =  (logNode) input_arr[0];
	
	node.setSchema(first_input.getSchema());
	node.setVarTbl(first_input.getVarTbl());
    }

    void handleDup(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

	dupOp op = new dupOp();
	op.setSelectedAlgoIndex(0);
	
	String inputAttr = e.getAttribute("input");
	logNode input = (logNode) ids2nodes.get(inputAttr);

	String branchAttr = e.getAttribute("branch");
	int branch = Integer.parseInt(branchAttr);
	op.setDup(branch);

	logNode node = new logNode(op, input);
	ids2nodes.put(id, node);

	// Nothing changes for the variables
	// Just use whatever variable table the node downstream has
	node.setSchema(input.getSchema());
	node.setVarTbl(input.getVarTbl());
    }

    void handleSort(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

	SortOp op = new SortOp();
	op.setSelectedAlgoIndex(0);
	
	String inputAttr = e.getAttribute("input");
	NodeList children = e.getChildNodes();
	
	logNode input =  (logNode) ids2nodes.get(inputAttr);
	varTbl qpVarTbl = input.getVarTbl();

	String sortbyAttr = e.getAttribute("sort_by");
	schemaAttribute sa = new schemaAttribute(qpVarTbl.lookUp(sortbyAttr));

	short comparisonMethod;
	String comparisonAttr = e.getAttribute("comparison");
	if (comparisonAttr.equals("alphabetic"))
	    comparisonMethod = SortOp.ALPHABETIC_COMPARISON;
	else
	    comparisonMethod = SortOp.NUMERIC_COMPARISON;

	boolean ascending;
	String orderAttr = e.getAttribute("order");
	if (orderAttr.equals("descending"))
	    ascending = false;
	else
	    ascending = true;
	op.setSort(sa, comparisonMethod, ascending);
	
	logNode sortNode = new logNode(op, input);
	ids2nodes.put(id, sortNode);
	
	// Nothing changes for the variables
	// Just use whatever variable table the node downstream has
	sortNode.setSchema(input.getSchema());
	sortNode.setVarTbl(qpVarTbl);
    }

    void handleJoin(Element e, Vector inputs) throws InvalidPlanException {
	String id = e.getAttribute("id");

	joinOp op = new joinOp();
	String className = e.getAttributeNode("physical").getValue();
	try {
	    op.setSelectedAlgorithm(className);
	} catch (ClassNotFoundException ex) {
	    throw new InvalidPlanException("Invalid algorithm: " + className
					   + "  " + ex.getMessage());
	} catch (niagara.xmlql_parser.op_tree.op.InvalidAlgorithmException x) {
	    throw new InvalidPlanException("Invalid algorithm: " + className
					   + "  " + x.getMessage());
	}
	
	logNode left = (logNode) ids2nodes.get(inputs.get(0));
	logNode right = (logNode) ids2nodes.get(inputs.get(1));
	
	NodeList children = e.getChildNodes();
	Element predElt = null;
	
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i) instanceof Element) {
		predElt = (Element) children.item(i);
		break;
	    }
	}
	
	varTbl leftv = left.getVarTbl();
	varTbl rightv = right.getVarTbl();
	
	predicate pred = parsePreds(predElt, leftv, rightv);

	// In case of an equijoin we may have to parse "left" 
	// and "right" to get additional equality predicates
	String leftattrs = e.getAttribute("left");
	String rightattrs = e.getAttribute("right");

	if (!leftattrs.equals("")) {
	    Vector leftvect = new Vector();
	    Vector rightvect = new Vector();
	    try {
		RE re = new RE("\\$[a-zA-Z0-9]*");
		REMatch[] all_left = re.getAllMatches(leftattrs);
		REMatch[] all_right = re.getAllMatches(rightattrs);
		for (int i = 0; i < all_left.length; i++) {
		    leftvect.addElement(leftv.lookUp(all_left[i].toString()));
		    rightvect.addElement(rightv.lookUp(all_right[i].toString()));

		    schemaAttribute sa = (schemaAttribute)leftvect.get(0);
		    if(sa == null) {
                       System.out.println("null sa");
		    }
		}
		op.setJoin(pred, leftvect, rightvect);
	    }
	    catch (REException rx) {
		System.out.println("Syntax error in regular expression.");
		rx.printStackTrace();
	    }
	}
	else 
	    op.setJoin(pred);
	
	logNode joinNode = new logNode(op, left, right);
	ids2nodes.put(id, joinNode);
	
	//Merge the two schemas 
	Schema sc = Util.mergeSchemas(left.getSchema(), right.getSchema());
	joinNode.setSchema(sc);
	
	//Merge the two varTbls 
	joinNode.setVarTbl(Util.mergeVarTbl(leftv, rightv, left.getSchema().numAttr()));
    }

    void handleAvg(Element e) throws InvalidPlanException {
	averageOp op = new averageOp();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");
	String groupby = e.getAttribute("groupby");
	String avgattr = e.getAttribute("avgattr");
	String inputAttr = e.getAttribute("input");
	
	logNode input = (logNode) ids2nodes.get(inputAttr);
	varTbl oldVarTbl = input.getVarTbl();

	varTbl qpVarTbl = new varTbl();
	Schema sc = new Schema();

	// Parse the groupby attribute to see what to group on
	Vector groupbySAs = new Vector();
	StringTokenizer st = new StringTokenizer(groupby);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    schemaAttribute oldAttr = oldVarTbl.lookUp(varName);
	    
	    groupbySAs.addElement(oldAttr);
	    qpVarTbl.addVar(varName, new schemaAttribute(groupbySAs.size()-1));
	    sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()-1));
	}

	schemaAttribute averagingAttribute = oldVarTbl.lookUp(avgattr);

	op.setAverageInfo(new skolem("avg", groupbySAs), averagingAttribute);
	
	//The attribute we're going to add to the result
	schemaAttribute resultSA = new schemaAttribute(groupbySAs.size());
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()));
	
	logNode avgNode = new logNode(op, input);
	
	ids2nodes.put(id, avgNode);
	
	avgNode.setSchema(sc);
	avgNode.setVarTbl(qpVarTbl);
    }

    void handleSum(Element e) throws InvalidPlanException {
	SumOp op = new SumOp();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");
	String groupby = e.getAttribute("groupby");
	String sumattr = e.getAttribute("sumattr");
	String inputAttr = e.getAttribute("input");
	
	logNode input = (logNode) ids2nodes.get(inputAttr);
	varTbl oldVarTbl = input.getVarTbl();

	varTbl qpVarTbl = new varTbl();
	Schema sc = new Schema();

	// Parse the groupby attribute to see what to group on
	Vector groupbySAs = new Vector();
	StringTokenizer st = new StringTokenizer(groupby);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    schemaAttribute oldAttr = oldVarTbl.lookUp(varName);
	    groupbySAs.addElement(oldAttr);
	    qpVarTbl.addVar(varName, new schemaAttribute(groupbySAs.size()-1));
	    sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()-1));
	}

	schemaAttribute summingAttribute = oldVarTbl.lookUp(sumattr);

	op.setSummingInfo(new skolem("sum", groupbySAs), summingAttribute);
	
	//The attribute we're going to add to the result
	schemaAttribute resultSA = new schemaAttribute(groupbySAs.size());
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()));

	logNode sumNode = new logNode(op, input);
	
	ids2nodes.put(id, sumNode);
	
	sumNode.setSchema(sc);
	sumNode.setVarTbl(qpVarTbl);
    }

    void handleCount(Element e) throws InvalidPlanException {
	CountOp op = new CountOp();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");
	String groupby = e.getAttribute("groupby");
	String countattr = e.getAttribute("countattr");
	String inputAttr = e.getAttribute("input");
	
	logNode input = (logNode) ids2nodes.get(inputAttr);
	varTbl oldVarTbl = input.getVarTbl();

	varTbl qpVarTbl = new varTbl();
	Schema sc = new Schema();

	// Parse the groupby attribute to see what to group on
	Vector groupbySAs = new Vector();
	StringTokenizer st = new StringTokenizer(groupby);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    schemaAttribute oldAttr = oldVarTbl.lookUp(varName);
	    groupbySAs.addElement(oldAttr);
	    qpVarTbl.addVar(varName, new schemaAttribute(groupbySAs.size()-1));
	    sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()-1));
	}

	schemaAttribute countingAttribute = oldVarTbl.lookUp(countattr);

	op.setCountingInfo(new skolem("count", groupbySAs), countingAttribute);
	
	//The attribute we're going to add to the result
	schemaAttribute resultSA = new schemaAttribute(groupbySAs.size());
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()));
	
	logNode countNode = new logNode(op, input);
	
	ids2nodes.put(id, countNode);
	
	countNode.setSchema(sc);
	countNode.setVarTbl(qpVarTbl);
    }

    void handleDtdScan(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

	// The node's children contain URLs
	Vector urls = new Vector();
	NodeList children = ((Element) e).getChildNodes();
	for (int i=0; i < children.getLength(); i++) {
	    Node child = children.item(i);
	    if (child.getNodeType() != Node.ELEMENT_NODE)
		continue;
	    urls.addElement(((Element) child).getAttribute("value"));
	}
	
	dtdScanOp op = new dtdScanOp();
	op.setDocs(urls);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);

	// Create new varTbl and schema
	varTbl qpVarTbl = new varTbl();
	
	schemaAttribute resultSA = 
	    new schemaAttribute(0, varType.ELEMENT_VAR);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	// Create a new, empty, schema
	Schema sc = new Schema();
	sc.addSchemaUnit(new SchemaUnit(null, 0));

	// Register schema and variable table for the
	// nodes above
	node.setSchema(sc);
	node.setVarTbl(qpVarTbl);
    }

    void handleResource(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");
        String urn = e.getAttribute("urn");
	
	ResourceOp op = new ResourceOp();
	op.setURN(urn);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);

	// Create new varTbl and schema
	varTbl qpVarTbl = new varTbl();
	
	schemaAttribute resultSA = 
	    new schemaAttribute(0, varType.ELEMENT_VAR);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	// Create a new, empty, schema
	Schema sc = new Schema();
	sc.addSchemaUnit(new SchemaUnit(null, 0));

	// Register schema and variable table for the
	// nodes above
	node.setSchema(sc);
	node.setVarTbl(qpVarTbl);
    }

    void handleConstant(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");
        String content = "";
        NodeList children = e.getChildNodes();
	for (int i=0; i < children.getLength(); i++) {
	    content = content + XMLUtils.flatten(children.item(i));
	}

	// Create new varTbl and schema
	varTbl qpVarTbl = new varTbl();
	
	Schema sc = new Schema();
	sc.addSchemaUnit(new SchemaUnit(null, 0));

        String vars = e.getAttribute("vars");
	// Parse the vars attribute 
	Vector varsAs = new Vector();
	StringTokenizer st = new StringTokenizer(vars);
        int pos = 0;
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    qpVarTbl.addVar(varName, new schemaAttribute(pos));
	    sc.addSchemaUnit(new SchemaUnit(null, pos));
            pos++;
	}

        ConstantOp op = new ConstantOp();
        op.setContent(content);

  	op.setSelectedAlgoIndex(0);
  	logNode node = new logNode(op);
  	ids2nodes.put(id, node);	    


	// Register schema and variable table for the
	// nodes above
	node.setSchema(sc);
	node.setVarTbl(qpVarTbl);

    }

    void handleSend(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

        SendOp op = new SendOp();

  	op.setSelectedAlgoIndex(0);

	String inputAttr = e.getAttribute("input");
	logNode input =  (logNode) ids2nodes.get(inputAttr);

  	logNode node = new logNode(op, input);
  	ids2nodes.put(id, node);	    
    }

    void handleDisplay(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

        DisplayOp op = new DisplayOp();

  	op.setSelectedAlgoIndex(0);
        op.setQueryId(e.getAttribute("query_id"));
        op.setClientLocation(e.getAttribute("client_location"));

	String inputAttr = e.getAttribute("input");
	logNode input =  (logNode) ids2nodes.get(inputAttr);

  	logNode node = new logNode(op, input);
  	ids2nodes.put(id, node);	    
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
        boolean streaming = e.getAttribute("streaming").equalsIgnoreCase("yes");
        boolean prettyPrint = e.getAttribute("prettyprint").equalsIgnoreCase("yes");
        String trace = e.getAttribute("trace");

	int dataType = -1;
	boolean found = false;
	for(int i = 0; i< FirehoseConstants.numDataTypes; i++) {
	    if(dataTypeStr.equalsIgnoreCase(FirehoseConstants.typeNames[i])) {
		dataType = i;
		found = true;
		break;
	    }
	}
	if(found == false)
	    throw new InvalidPlanException("Invalid type - typeStr: " + 
					   dataTypeStr);
		 
	FirehoseSpec fhspec = 
	    new FirehoseSpec(port, host, dataType, descriptor, descriptor2,
	                     numGenCalls, numTLElts, rate, streaming,
			     prettyPrint, trace);
	
	FirehoseScanOp op = new FirehoseScanOp();
	op.setSelectedAlgoIndex(0);
	op.setFirehoseScan(fhspec);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);	    
    }

    void handleFileScan(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");
	boolean isStreaming = e.getAttribute("streaming").equals("yes");
	FileScanSpec fsSpec = new FileScanSpec(e.getAttribute("fileName"), isStreaming); 

	FileScanOp op = new FileScanOp();
	op.setSelectedAlgoIndex(0);
	op.setFileScan(fsSpec);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);	    
    }

    void handleConstruct(Element e) throws InvalidPlanException {
	String id = e.getAttribute("id");

	constructOp op = new constructOp();
	op.setSelectedAlgoIndex(0);
	NodeList children = e.getChildNodes();
	String content = "";
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i).getNodeType() != Node.ELEMENT_NODE)
		continue;
	    content = content + XMLUtils.explosiveFlatten(children.item(i));
	}
	String inputAttr = e.getAttribute("input");
	
	logNode input = (logNode) ids2nodes.get(inputAttr);
	Scanner scanner;
	constructBaseNode cn = null;
	
	try {
	    scanner = new Scanner(new StringReader(content));
	    ConstructParser cep = new ConstructParser(scanner);
	    cn = (constructBaseNode) cep.parse().value;
	    cep.done_parsing();
	}
	catch (Exception ex) {
	    throw new InvalidPlanException("Error while parsing: "+ content);
	}

	varTbl qpVarTbl = null; 
	Schema sc = null;

	boolean clear = false;
	if (e.getAttribute("clear").equals("yes"))
	    clear = true;
	
	if (input.getVarTbl() != null) {
	    qpVarTbl = new varTbl(input.getVarTbl());
	    sc = new Schema(input.getSchema()); 
	    cn.replaceVar(qpVarTbl);
	}

	if (input.getVarTbl() == null || clear) {
	    qpVarTbl = new varTbl();
	    sc = new Schema();
	}

	op.setConstruct(cn, clear);
	
	logNode node = new logNode(op, (logNode) ids2nodes.get(inputAttr));

	int nextVar;
	if (input.getVarTbl() == null || clear) {
	    nextVar = 0;
	}
	else {
	    nextVar = sc.numAttr();
	}

	schemaAttribute resultSA = 
	    new schemaAttribute(nextVar, varType.ELEMENT_VAR);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	// Update the schema
	sc.addSchemaUnit(new SchemaUnit(null, nextVar));
	
	ids2nodes.put(id, node);

	// Register schema and variable table for the
	// nodes above
	node.setSchema(sc);
	node.setVarTbl(qpVarTbl);
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
	    op.setSelectedAlgoIndex(0);
	    
	    String clear = e.getAttribute("clear");
	    boolean cl;
	    if(clear.equals("false")) {
		cl = false;
	    } else {
		cl = true;
	    }

	    varTbl qpVarTbl = null; 
	    logNode inputNode = (logNode) ids2nodes.get(inputAttr);
	    
	    if (inputNode.getVarTbl() != null) {
		qpVarTbl = new varTbl(inputNode.getVarTbl());
	    } else {
		throw new InvalidPlanException("Accumulate needs variable " + mergeAttr 
					       + "but input variable table is null");
	    }
	    
	    schemaAttribute mergeSA;
	    mergeSA = new schemaAttribute(qpVarTbl.lookUp(mergeAttr));
	    op.setAccumulate(mergeTemplate, mergeSA, accumFileName,
			     initialAccumFile, cl);

	    logNode accumNode = new logNode(op, inputNode);
	    ids2nodes.put(id, accumNode);	   

	    // PhysicalAccumulateOperator outputs single-element StreamTuplElements
	    // Create a new varTbl, containing a single variable for this element
	    qpVarTbl = new varTbl();

	    schemaAttribute resultSA = 
		new schemaAttribute(0, varType.ELEMENT_VAR);
	
	    // Register variable -> resultSA
	    qpVarTbl.addVar("$" + id, resultSA);

	    // Create a new, empty, schema
	    Schema sc = new Schema();
	    sc.addSchemaUnit(new SchemaUnit(null, 0));

	    // Register schema and variable table for the
	    // nodes above
	    accumNode.setSchema(sc);
	    accumNode.setVarTbl(qpVarTbl);
	} catch (MTException mte) {
	    throw new InvalidPlanException("Invalid Merge Template" + 
					   mte.getMessage());
	}
    }


    // XXX This code should be rewritten to take advantage of 
    // XXX getName and getCode in opType
    predicate parsePreds(Element e, varTbl leftv, varTbl rightv) {
	predicate p;

	if (e == null)
	    return null;

        Element l, r;
        l = r = null;

        Node c = e.getFirstChild();
        do {
            if (c.getNodeType() == Node.ELEMENT_NODE) {
                if (l == null) l = (Element) c;
                else if (r == null) r = (Element) c;
            }
            c = c.getNextSibling();
        } while (c != null);

	if (e.getNodeName().equals("and")) {
	    predicate left = parsePreds(l, leftv, rightv);
	    predicate right = parsePreds(r, leftv, rightv);

	    return new predLogOpNode(opType.AND, left, right);
	}
	else if (e.getNodeName().equals("or")) {
	    predicate left = parsePreds(l, leftv, rightv);
	    predicate right = parsePreds(r, leftv, rightv);

	    return new predLogOpNode(opType.OR, left, right);
	}
	else if (e.getNodeName().equals("not")) {
	    predicate child = parsePreds(l, leftv, rightv);

	    return new predLogOpNode(opType.NOT, child);
	}
	else { // Relational operator
	    data left = parseAtom(l, leftv, rightv);
	    data right = parseAtom(r, leftv, rightv);

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
	    else if (op.equals("contain"))
		type = opType.CONTAIN;
	    else // eq
		type = opType.EQ;

            // XXX horrible hack for toXML()
            predArithOpNode toReturn = new predArithOpNode(type, left, right);
            Vector varlist = new Vector();
            if (left.getType() == dataType.ATTR) {
                varlist.add(l.getAttribute("value"));
            }
            if (right.getType() == dataType.ATTR) {
                varlist.add(r.getAttribute("value"));
            }
            toReturn.setVarList(varlist);
	    return toReturn;
	}
    }

    data parseAtom(Element e, varTbl left, varTbl right) {
	if (e.getNodeName().equals("number"))
	    return new data(dataType.NUMBER, e.getAttribute("value"));
	else if (e.getNodeName().equals("string"))
	    return new data(dataType.STRING, e.getAttribute("value"));
	else { //var 
	    String varname = e.getAttribute("value");
	    if (left.lookUp(varname) != null)
		return new data(dataType.ATTR, left.lookUp(varname));	    
	    else {
		schemaAttribute sa = new schemaAttribute(right.lookUp(varname));
		sa.setStreamId(1);
		return new data(dataType.ATTR, sa);
	    }
	}

    }


    public class InvalidPlanException extends Exception {
	public InvalidPlanException() {
	    super("Invalid Plan Exception: ");
	}
	public InvalidPlanException(String msg) {
	    super("Invalid Plan Exception: " + msg + " ");
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

	logNode input = (logNode) ids2nodes.get(inputAttr);
	varTbl oldVarTbl = input.getVarTbl();

	varTbl qpVarTbl = new varTbl();
	Schema sc = new Schema();

	// Parse the groupby attribute to see what to group on
	Vector groupbySAs = new Vector();
	StringTokenizer st = new StringTokenizer(groupby);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    schemaAttribute oldAttr = oldVarTbl.lookUp(varName);
	    
	    groupbySAs.addElement(oldAttr);
	    qpVarTbl.addVar(varName, new schemaAttribute(groupbySAs.size()-1));
	    sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()-1));
	}

	schemaAttribute maxAttribute = oldVarTbl.lookUp(maxattr);

	op.setSkolemAttributes(new skolem("incrmax", groupbySAs));
	op.setMaxAttribute(maxAttribute);
	op.setEmptyGroupValue(Double.valueOf(emptyGroupValueAttr));

	// Old/New result attributes
	schemaAttribute oldSA = new schemaAttribute(groupbySAs.size(), 
						    varType.ELEMENT_VAR);
	schemaAttribute newSA = new schemaAttribute(groupbySAs.size()+1,
						    varType.ELEMENT_VAR);
	
	// Register variables
	qpVarTbl.addVar("$old" + id, oldSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()));
	qpVarTbl.addVar("$" + id, newSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size() + 1));
	
	logNode maxNode = new logNode(op, input);
	
	ids2nodes.put(id, maxNode);
	
	maxNode.setSchema(sc);
	maxNode.setVarTbl(qpVarTbl);
    }

    void handleIncrAvg(Element e) throws InvalidPlanException {
	IncrementalAverage op = new IncrementalAverage();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");
	String groupby = e.getAttribute("groupby");
	String avgattr = e.getAttribute("avgattr");
	String inputAttr = e.getAttribute("input");

	logNode input = (logNode) ids2nodes.get(inputAttr);
	varTbl oldVarTbl = input.getVarTbl();

	varTbl qpVarTbl = new varTbl();
	Schema sc = new Schema();

	// Parse the groupby attribute to see what to group on
	Vector groupbySAs = new Vector();
	StringTokenizer st = new StringTokenizer(groupby);
	while (st.hasMoreTokens()) {
	    String varName = st.nextToken();

	    schemaAttribute oldAttr = oldVarTbl.lookUp(varName);
	    
	    groupbySAs.addElement(oldAttr);
	    qpVarTbl.addVar(varName, new schemaAttribute(groupbySAs.size()-1));
	    sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()-1));
	}

	schemaAttribute avgAttribute = oldVarTbl.lookUp(avgattr);

	op.setSkolemAttributes(new skolem("incravg", groupbySAs));
	op.setAvgAttribute(avgAttribute);

	// New result attribute
	schemaAttribute newSA = new schemaAttribute(groupbySAs.size(),
						    varType.ELEMENT_VAR);
	
	// Register variables
	qpVarTbl.addVar("$" + id, newSA);
	sc.addSchemaUnit(new SchemaUnit(null, groupbySAs.size()));
	
	logNode avgNode = new logNode(op, input);
	
	ids2nodes.put(id, avgNode);
	
	avgNode.setSchema(sc);
	avgNode.setVarTbl(qpVarTbl);
    }
}
