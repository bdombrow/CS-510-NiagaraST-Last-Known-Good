package niagara.client.qpclient;

import diva.graph.*;
import diva.graph.model.*;
import diva.util.*;

import java.util.*;
import java.io.*;

import org.w3c.dom.*;
import niagara.utils.XMLUtils;

public class Join extends Operator {
    String content = "";
    public Join() {
	super();
	type = "join";
    }
    protected void saveChildren(PrintWriter pw, GraphView gv) {
	pw.println(content);
    }

    public void handleChildren(Element e, IDREFResolver idr, GraphModel gm) {
	NodeList nl = e.getChildNodes();
	for (int i=0; i < nl.getLength(); i++) {
	    org.w3c.dom.Node n = nl.item(i);
	    if (!(n instanceof Element))
		continue;
	    content = content + XMLUtils.flatten(n, false);
	}
    }
}
