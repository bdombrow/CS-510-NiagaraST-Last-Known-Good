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

    // Maps variables to schema attributes
    varTbl qpVarTbl;

    public void initialize() {
	ids2els = new Hashtable();
	ids2nodes = new Hashtable();
	qpVarTbl = new varTbl();
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

    logNode parsePlan(Element root) throws InvalidPlanException, CloneNotSupportedException {
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

    void parseOperator(Element e) 
	throws InvalidPlanException, CloneNotSupportedException {
	String id = e.getAttribute("id");

	// If we already visited this node, just return 
	if (ids2nodes.containsKey(id))
	    return;
		      
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
	if (e.getNodeName().equals("scan")) {

	    scanOp op = (scanOp) operators.Scan.clone();
	    op.setSelectedAlgoIndex(0);

	    String indexAttr = e.getAttribute("index");
	    String inputAttr = e.getAttribute("input");
	    String typeAttr = e.getAttribute("type");
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
	    
	    schemaAttribute resultSA = 
		new schemaAttribute(Integer.parseInt(indexAttr) + 1, type);
	    // Register variable -> resultSA
	    qpVarTbl.addVar("$" + id, resultSA);

	    schemaAttribute sa = 
		new schemaAttribute(Integer.parseInt(indexAttr));

	    Scanner scanner;
	    regExp redn = null;
	    try {
		scanner = new Scanner(new StringReader(regexpAttr));
		REParser rep = new REParser(scanner);
		redn = (regExp) rep.parse().value;
		rep.done_parsing();
	    }
	    catch (Exception ex) {
		System.err.println("Error while parsing: "+ regexpAttr);
		ex.printStackTrace();
		throw new InvalidPlanException();
	    }
	    op.setScan(sa, redn);

	    logNode scanNode = new logNode(op, 
					   (logNode) ids2nodes.get(inputAttr));
	    ids2nodes.put(id, scanNode);
	}

	else if (e.getNodeName().equals("select")) {
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

	    predicate pred = parsePreds(predElt);

	    Vector v = new Vector(); v.addElement(pred);
	    op.setSelect(v);

	    logNode selectNode = new logNode(op, 
					   (logNode) ids2nodes.get(inputAttr));
	    ids2nodes.put(id, selectNode);
	}

	else if (e.getNodeName().equals("dtdscan")) {
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

	else if (e.getNodeName().equals("construct")) {
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
	    
	    cn.replaceVar(qpVarTbl);
	    op.setConstruct(cn);

	    logNode node = new logNode(op, (logNode) ids2nodes.get(inputAttr));
	    ids2nodes.put(id, node);
	}
	else if (e.getNodeName().equals("firehosescan")) {
	    System.out.println("firehosescan");

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
    }

    predicate parsePreds(Element e) {
	predicate p;

	NodeList children = e.getChildNodes();
	// Remove superfluous Text Nodes!
	for (int i=0; i < children.getLength(); i++) {
	    if (!(children.item(i) instanceof Element)) {
		e.removeChild(children.item(i));
	    }
	}

	if (e.getNodeName().equals("and")) {
	    predicate left = parsePreds((Element) e.getChildNodes().item(0));
	    predicate right = parsePreds((Element) e.getChildNodes().item(1));

	    p = new predLogOpNode(opType.AND, left, right);
	    return p;
	}
	else if (e.getNodeName().equals("or")) {
	    predicate left = parsePreds((Element) e.getChildNodes().item(0));
	    predicate right = parsePreds((Element) e.getChildNodes().item(1));

	    p = new predLogOpNode(opType.OR, left, right);
	    return p;
	}
	else if (e.getNodeName().equals("not")) {
	    predicate child = parsePreds((Element) e.getChildNodes().item(0));

	    p = new predLogOpNode(opType.NOT, child);
	    return p;
	}
	else { // Relational operator
	    data left = parseAtom((Element) e.getChildNodes().item(0));
	    data right = parseAtom((Element) e.getChildNodes().item(1));

	    int type;
	    if (e.getAttribute("op").equals("lt"))
		type = opType.LT;
	    else if (e.getAttribute("op").equals("gt"))
		type = opType.GT;
	    else if (e.getAttribute("op").equals("le"))
		type = opType.LEQ;
	    else if (e.getAttribute("op").equals("ge"))
		type = opType.GEQ;
	    else if (e.getAttribute("op").equals("ne"))
		type = opType.NEQ;
	    else // eq
		type = opType.EQ;

	    p = new predArithOpNode(type, left, right);
	    return p;
	}
    }

    data parseAtom(Element e) {
	if (e.getNodeName().equals("number"))
	    return new data(dataType.NUMBER, e.getAttribute("value"));
	else if (e.getNodeName().equals("string"))
	    return new data(dataType.STRING, e.getAttribute("value"));
	else // var 
	    return new data(dataType.ATTR, new schemaAttribute(Integer.parseInt(e.getAttribute("value").substring(1))));
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
