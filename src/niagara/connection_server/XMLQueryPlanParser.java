/** Generate a physical plan from an XML Description
 *
 */
package niagara.connection_server;

import com.ibm.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;
import java.util.*;


import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.xmlql_parser.syntax_tree.re_parser.*;
import niagara.xmlql_parser.syntax_tree.construct_parser.*;

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
	RevalidatingDOMParser p;
	Document document = null;
	try {
	    p = new RevalidatingDOMParser();
	    p.parse(new InputSource(new ByteArrayInputStream(description.getBytes())));
	    document = p.getDocument();
	    Element root = (Element) document.getDocumentElement();
	    return parsePlan(root);
	}
	catch (Exception e) {
	    e.printStackTrace();
	    throw new InvalidPlanException();
	}
    }

    logNode parsePlan(Element root) 
	throws InvalidPlanException, CloneNotSupportedException 
    {
	NodeList children = root.getChildNodes();
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i).getNodeType() != Node.ELEMENT_NODE)
		continue;
	    Element e = (Element) children.item(i);
	    ids2els.put(e.getAttribute("id"), e);
	}

	String top = root.getAttribute("top");
	parseOperator((Element) ids2els.get(top));
	return (logNode) ids2nodes.get(top);
    }

    /**
     * @param e an <code>Element</code> describing a logical plan operator
     * @exception InvalidPlanException the description of the plan was invalid
     * @exception CloneNotSupportedException 
     */
    void parseOperator(Element e) 
	throws InvalidPlanException, CloneNotSupportedException {
	String id = e.getAttribute("id");

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
	    Element el = (Element) ids2els.get(inputs.elementAt(i));
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
	else if (nodeName.equals("join")) {
	    handleJoin(e, inputs);
	}
	else if (nodeName.equals("avg")) {
	    handleAvg(e);
	}
	else if (nodeName.equals("dtdscan")) {
	    handleDtdScan(e);
	}
	else if (nodeName.equals("construct")) {
	    handleConstruct(e);
	}
	else if (nodeName.equals("firehosescan")) {
	    handleFirehoseScan(e);
	}
    }

    void handleScan(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
	scanOp op = (scanOp) operators.Scan.clone();
	op.setSelectedAlgoIndex(0);

	String id = e.getAttribute("id");

	String inputAttr = e.getAttribute("input");
	String typeAttr = e.getAttribute("type");
	String rootAttr = e.getAttribute("root");
	String regexpAttr = e.getAttribute("regexp");
	
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
	}
	catch (Exception ex) {
	    System.err.println("Error while parsing: " + regexpAttr);
	    ex.printStackTrace();
	    throw new InvalidPlanException();
	}
	
	schemaAttribute resultSA = 
	    new schemaAttribute(sc.numAttr(), type);
	
	// Register variable -> resultSA
	qpVarTbl.addVar("$" + id, resultSA);

	// The attribute the regexp starts from
	// If left blank, we start the regexp from the last
	// attribute added to the input tuple
	schemaAttribute sa;
	if (rootAttr.equals(""))
	    sa = new schemaAttribute(sc.numAttr()-1);
	else
	    sa = new schemaAttribute(qpVarTbl.lookUp(rootAttr));

	op.setScan(sa, redn);

	sc.addSchemaUnit(new SchemaUnit(redn, sc.numAttr()));

	
	logNode scanNode = new logNode(op, input);
	
	ids2nodes.put(id, scanNode);
	
	scanNode.setSchema(sc);
	scanNode.setVarTbl(qpVarTbl);
    }

    void handleSelect(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
	String id = e.getAttribute("id");

	selectOp op = (selectOp) operators.Select.clone();
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
	
	// Nothing changes for the variables
	// Just use whatever variable table the node downstream has
	selectNode.setSchema(input.getSchema());
	selectNode.setVarTbl(qpVarTbl);
    }

    void handleJoin(Element e, Vector inputs) 
	throws CloneNotSupportedException, InvalidPlanException {
	String id = e.getAttribute("id");

	joinOp op = (joinOp) operators.Join.clone();
	String className = e.getAttributeNode("physical").getValue();
	try {
	    op.setSelectedAlgorithm(className);
	}
	catch (Exception ex) {
	    System.err.println("Invalid algorithm: " + className);
	    throw new InvalidPlanException();
	}
	
	logNode left = (logNode) ids2nodes.get(inputs.elementAt(0));
	logNode right = (logNode) ids2nodes.get(inputs.elementAt(1));
	
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
	op.setJoin(pred);
	
	logNode joinNode = new logNode(op, left, right);
	ids2nodes.put(id, joinNode);
	
	//Merge the two schemas 
	Schema sc = Util.mergeSchemas(left.getSchema(), right.getSchema());
	joinNode.setSchema(sc);
	
	//Merge the two varTbls 
	joinNode.setVarTbl(Util.mergeVarTbl(leftv, rightv, left.getSchema().numAttr()));
    }

    void handleAvg(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
	averageOp op = (averageOp) operators.Average.clone();
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
	
	logNode avgNode = new logNode(op, input);
	
	ids2nodes.put(id, avgNode);
	
	avgNode.setSchema(sc);
	avgNode.setVarTbl(qpVarTbl);
    }

    void handleDtdScan(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
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
	
	dtdScanOp op = (dtdScanOp) operators.DtdScan.clone();
	op.setDocs(urls);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);
    }

    void handleFirehoseScan(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
	String id = e.getAttribute("id");

	String host = e.getAttribute("host");
	int port = Integer.parseInt(e.getAttribute("port"));
	int rate = Integer.parseInt(e.getAttribute("rate"));
	int iters = Integer.parseInt(e.getAttribute("iters"));
	String typeStr = e.getAttribute("type");
	int type;
	// XXX FIXME we should import FirehoseClient to get to these values
	if (typeStr.equals("file"))
	    type = 1;
	else // gen
	    type = 2;
	String desc = e.getAttribute("desc");
	FirehoseSpec fhspec = 
	    new FirehoseSpec(port, host, rate, type, desc, iters);
	
	FirehoseScanOp op = 
	    (FirehoseScanOp) operators.FirehoseScan.clone();
	
	op.setFirehoseScan(fhspec);
	logNode node = new logNode(op);
	ids2nodes.put(id, node);	    
    }

    void handleConstruct(Element e) 
	throws CloneNotSupportedException, InvalidPlanException {
	String id = e.getAttribute("id");

	constructOp op = (constructOp) operators.Construct.clone();
	op.setSelectedAlgoIndex(0);
	NodeList children = e.getChildNodes();
	String content = "";
	for (int i=0; i < children.getLength(); i++) {
	    if (children.item(i).getNodeType() != Node.ELEMENT_NODE)
		continue;
	    content = content + flatten(children.item(i));
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
	    System.err.println("Error while parsing: "+ content);
	    throw new InvalidPlanException();
	}
	
	varTbl qpVarTbl = input.getVarTbl();
	
	cn.replaceVar(qpVarTbl);
	op.setConstruct(cn);
	
	logNode node = new logNode(op, (logNode) ids2nodes.get(inputAttr));
	
	ids2nodes.put(id, node);
	node.setVarTbl(qpVarTbl);
    }

    predicate parsePreds(Element e, varTbl leftv, varTbl rightv) {
	predicate p;

	if (e == null)
	    return null;

	NodeList children = e.getChildNodes();
	// Remove superfluous Text Nodes!
	for (int i=0; i < children.getLength(); i++) {
	    if (!(children.item(i) instanceof Element)) {
		e.removeChild(children.item(i));
	    }
	}

	if (e.getNodeName().equals("and")) {
	    predicate left = parsePreds((Element) e.getChildNodes().item(0), leftv, rightv);
	    predicate right = parsePreds((Element) e.getChildNodes().item(1), leftv, rightv);

	    return new predLogOpNode(opType.AND, left, right);
	}
	else if (e.getNodeName().equals("or")) {
	    predicate left = parsePreds((Element) e.getChildNodes().item(0), leftv, rightv);
	    predicate right = parsePreds((Element) e.getChildNodes().item(1), leftv, rightv);

	    return new predLogOpNode(opType.OR, left, right);
	}
	else if (e.getNodeName().equals("not")) {
	    predicate child = parsePreds((Element) e.getChildNodes().item(0), leftv, rightv);

	    return new predLogOpNode(opType.NOT, child);
	}
	else { // Relational operator
	    data left = parseAtom((Element) e.getChildNodes().item(0), leftv, rightv);
	    data right = parseAtom((Element) e.getChildNodes().item(1), leftv, rightv);

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
	    else // eq
		type = opType.EQ;

	    return new predArithOpNode(type, left, right);
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
		return new data(dataType.ATTR, left.lookUp(e.getAttribute("value")));	    
	    else {
		schemaAttribute sa = new schemaAttribute(right.lookUp(varname));
		sa.setStreamId(1);
		return new data(dataType.ATTR, sa);
	    }
	}

    }

    String flatten(Node n) {
	short type = n.getNodeType();
	if (type == Node.ELEMENT_NODE) {
	    String ret =  "<" + n.getNodeName() + ">";
	    NodeList nl = n.getChildNodes();
	    for (int i=0; i < nl.getLength(); i++) {
		ret = ret + flatten(nl.item(i));
	    }
	    return ret + "</" + n.getNodeName() + ">";
	}
	else if (type == Node.TEXT_NODE) {
	    return n.getNodeValue();
	}
	else 
	    return "";
    }

    public class InvalidPlanException extends Exception {}
}
