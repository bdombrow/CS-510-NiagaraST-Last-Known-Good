package niagara.utils;

import org.w3c.dom.*;

public class XMLUtils {

    public static int getInt(StreamTupleElement ste, int attrpos) {
	try {
	    String sval = ((Node) ste.getAttribute(attrpos)).
		getChildNodes().item(0).getNodeValue();
	    return Integer.parseInt(sval);
	}
	catch (Exception e) {
	    return 0;
	}
    }

    // XXX vpapad Must rewrite with StringBuffer
    public static String flatten(Node n) {
	short type = n.getNodeType();
	if (type == Node.ELEMENT_NODE) {
	    String ret =  "<" + n.getNodeName();
	    NamedNodeMap attrs = n.getAttributes();	    
	    for (int i=0; i < attrs.getLength(); i++) {
		Attr a = (Attr) attrs.item(i);
		ret = ret + " " + a.getName() + "=\"" + a.getValue() + "\"";
	    }
	    ret = ret + ">";
	    NodeList nl = n.getChildNodes();
	    for (int i=0; i < nl.getLength(); i++) {
                ret += flatten(nl.item(i));
	    }
	    return ret + "</" + n.getNodeName() + ">";
	}
	else if (type == Node.TEXT_NODE || type == Node.CDATA_SECTION_NODE) {
	    return n.getNodeValue();
	}
	else if (type == Node.DOCUMENT_NODE) {
            if (n.getFirstChild() != null)
                return flatten(n.getFirstChild());
            else
                return "<!-- Empty document node -->";
	}
	else
	    return "<!-- XMLUtils.flatten() could not serialize this node -->";
    }
}



