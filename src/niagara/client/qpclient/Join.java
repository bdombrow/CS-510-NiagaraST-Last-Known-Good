package niagara.client.qpclient;

import java.io.PrintWriter;

import niagara.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import diva.graph.GraphView;
import diva.graph.model.GraphModel;

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
