package niagara.utils;

import org.w3c.dom.*;

public class XMLUtils {
    static final String tb = "   ";
    static final String indent[] = {"", tb, tb+tb, tb+tb+tb, tb+tb+tb+tb,
                     tb+tb+tb+tb+tb, tb+tb+tb+tb+tb+tb,
		     tb+tb+tb+tb+tb+tb+tb,
		     tb+tb+tb+tb+tb+tb+tb+tb};

    public static int getInt(StreamTupleElement ste, int attrpos) {
        String sval =
            ((Node) ste.getAttribute(attrpos))
                .getChildNodes()
                .item(0)
                .getNodeValue();
        return Integer.parseInt(sval);
    }

    public static String flatten(Node n, boolean prettyprint) {
        StringBuffer sb = new StringBuffer();
        flatten(n, sb, false, prettyprint);
        return sb.toString();
    }

    public static String explosiveFlatten(Node n) {
        StringBuffer sb = new StringBuffer();
        flatten(n, sb, true, false);
        return sb.toString();
    }

    public static void flatten(Node n, StringBuffer sb,
                               boolean explode, boolean prettyprint) {
        flatten(n, sb, explode, prettyprint, 0, true);
    }

    public static void flatten(Node n, StringBuffer sb, boolean explode,
			       boolean prettyprint, int level, boolean onlychild) {
	short type = n.getNodeType();
        if (type == Node.ELEMENT_NODE) {
	    if(prettyprint) {
		sb.append(indent[level]);
	    }
            sb.append("<").append(n.getNodeName());

            NamedNodeMap attrs = n.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Attr a = (Attr) attrs.item(i);
                if (explode) {
                    String namespaceURI = a.getNamespaceURI();
                    if (namespaceURI != null
                        && namespaceURI.equals(
                            "http://www.cse.ogi.edu/dot/niagara/")
                        && a.getLocalName().equals("explode")) {
                        // Replace the attribute with its content
                        sb.append(" ").append(a.getValue());
                    } else
                        sb.append(" ").append(a.getName()).append(
                            "=\"").append(
                            a.getValue()).append(
                            "\"");
                } else
                    sb.append(" ").append(a.getName()).append("=\"").append(
                        a.getValue()).append(
                        "\"");
            }
            NodeList nl = n.getChildNodes();
            int nChildren = nl.getLength();
	    int nDecendents = nChildren; // not quite right, but works OK for now KT
	    boolean onekid = false;

            if (nChildren == 0) {
                sb.append("/>");
		if(prettyprint)
		    sb.append("\n");
	    } else {
                sb.append(">");
		if(prettyprint) {
		    if(nDecendents > 1 && prettyprint)
			sb.append("\n");
		}
		if(nChildren == 1)
		    onekid = true;
                for (int i = 0; i < nChildren; i++)
                    flatten(nl.item(i), sb, explode, prettyprint, level+1, onekid);
		if(nDecendents > 1 && prettyprint) {
		    sb.append(indent[level]);
		}
                sb.append("</").append(n.getNodeName()).append(">");
		if(prettyprint)
		    sb.append("\n");
            }
        } else if (type == Node.TEXT_NODE || type == Node.CDATA_SECTION_NODE) {
	    if(prettyprint && !onlychild)
		sb.append(indent[level]);
            sb.append(n.getNodeValue());
	    if(prettyprint && !onlychild)
		sb.append("\n");
 	} else if (type == Node.DOCUMENT_NODE) {
 	    Node kid = n.getFirstChild();
 	    boolean done = false;
             while (kid != null && done == false) {
 		short kidType = kid.getNodeType();
 		if(kidType == Node.TEXT_NODE ||
 		   kidType == Node.CDATA_SECTION_NODE ||
 		   kidType == Node.ELEMENT_NODE) {
 		    flatten(kid, sb, explode, prettyprint);
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


    public static Element getFirstElementChild(
        Element e,
        String tag,
        RuntimeException exc) {
        NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element result = (Element) n;
                if (result.getTagName().equals(tag))
                    return result;
            }
        }
        throw exc;
    }

    public static Element getOnlyElementChild(
        Element e,
        String tag,
        RuntimeException exc) {
        NodeList nl = e.getChildNodes();
        Element result = null;
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                if (result != null) // more than one Element children 
                    throw exc;
                result = (Element) n;
            }
        }

        if (result == null // zero Element children
            || !result.getTagName().equals(tag)) // wrong tag
            throw exc;
        return result;
    }
}
