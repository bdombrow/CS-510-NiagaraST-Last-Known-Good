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

    public static String flatten(Node n) {
        StringBuffer sb = new StringBuffer();
        flatten(n, sb);
        return sb.toString();
    }

    public static void flatten(Node n, StringBuffer sb) {
	short type = n.getNodeType();
	if (type == Node.ELEMENT_NODE) {
	    sb.append("<" + n.getNodeName());
	    NamedNodeMap attrs = n.getAttributes();	    
	    for (int i=0; i < attrs.getLength(); i++) {
		Attr a = (Attr) attrs.item(i);
		sb.append(" " + a.getName() + "=\"" + a.getValue() + "\"");
	    }
	    sb.append(">");
	    NodeList nl = n.getChildNodes();
	    for (int i=0; i < nl.getLength(); i++) {
                flatten(nl.item(i), sb);
	    }
	    sb.append("</" + n.getNodeName() + ">");
	}
	else if (type == Node.TEXT_NODE || type == Node.CDATA_SECTION_NODE) {
	    sb.append(n.getNodeValue());
	}
	else if (type == Node.DOCUMENT_NODE) {
            if (n.getFirstChild() != null)
                flatten(n.getFirstChild(), sb);
            else
                sb.append("<!-- Empty document node -->");
	}
	else
	    sb.append("<!-- XMLUtils.flatten() could not serialize this node -->");
    }
}



