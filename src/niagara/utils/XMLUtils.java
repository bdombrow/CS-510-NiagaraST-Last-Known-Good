package niagara.utils;

import org.w3c.dom.*;

public class XMLUtils {

    public static int getInt(StreamTupleElement ste, int attrpos) {
	//try {
	    String sval = ((Node) ste.getAttribute(attrpos)).
		getChildNodes().item(0).getNodeValue();
	    return Integer.parseInt(sval);
	    //}
	    //catch (Exception e) {
	    //return 0;
	    //}
    }

    public static String flatten(Node n) {
        StringBuffer sb = new StringBuffer();
        flatten(n, sb, false);
        return sb.toString();
    }

    public static String explosiveFlatten(Node n) {
        StringBuffer sb = new StringBuffer();
        flatten(n, sb, true);
        return sb.toString();
    }

    public static void flatten(Node n, StringBuffer sb, boolean explode) {
	short type = n.getNodeType();
	if (type == Node.ELEMENT_NODE) {
	    sb.append("<" + n.getNodeName());
	    NamedNodeMap attrs = n.getAttributes();	    
	    for (int i=0; i < attrs.getLength(); i++) {
		Attr a = (Attr) attrs.item(i);
		if (explode) {
		    String namespaceURI = a.getNamespaceURI();
		    if (namespaceURI != null && 
			namespaceURI.equals("http://www.cse.ogi.edu/dot/niagara/")
			&& a.getLocalName().equals("explode")) {
			// Replace the attribute with its content
			sb.append(" " + a.getValue());
		    }
		    else sb.append(" " + a.getName() + "=\"" + a.getValue() + "\"");
		}
		else 
		    sb.append(" " + a.getName() + "=\"" + a.getValue() + "\"");
	    }
	    sb.append(">");
	    NodeList nl = n.getChildNodes();
	    for (int i=0; i < nl.getLength(); i++) {
                flatten(nl.item(i), sb, explode);
	    }
	    sb.append("</" + n.getNodeName() + ">");
	}
	else if (type == Node.TEXT_NODE || type == Node.CDATA_SECTION_NODE) {
	    sb.append(n.getNodeValue());
 	} else if (type == Node.DOCUMENT_NODE) {
 	    Node kid = n.getFirstChild();
 	    boolean done = false;
             while (kid != null && done == false) {
 		short kidType = kid.getNodeType();
 		if(kidType == Node.TEXT_NODE ||
 		   kidType == Node.CDATA_SECTION_NODE ||
 		   kidType == Node.ELEMENT_NODE) {
 		    flatten(kid, sb, explode);
 		    done = true;
 		} else {
 		    kid = kid.getNextSibling();
 		}
 	    }
 	    if(done == false) {
 		sb.append("<!-- Empty document node -->");
 	    }
 	} else {
	    sb.append("<!-- XMLUtils.flatten() could not serialize this node -->");
 	    throw new RuntimeException("flatten" + type + " " + Node.DOCUMENT_NODE);
	}
    }
}



