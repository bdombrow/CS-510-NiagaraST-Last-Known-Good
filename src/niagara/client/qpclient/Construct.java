package niagara.client.qpclient;

import java.awt.Graphics2D;
import java.io.PrintWriter;

import niagara.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import diva.graph.GraphView;
import diva.graph.model.GraphModel;

public class Construct extends Operator {
    public Construct() {
	super();
	type = "construct";
    }

    protected void saveChildren(PrintWriter pw, GraphView gv) {
	pw.println((String) getProperty("_result"));
    }

    public void paint(QPFigure qpf, Graphics2D g) {
	LineDescription toDisplay[] = {
	    new LineDescription(type, CENTER, g.getFont()),
	};
	setLines(toDisplay);
	super.paint(qpf, g);
    }

    public void handleChildren(Element e, IDREFResolver idr, GraphModel gm) {
	String content = "";
	NodeList nl = e.getChildNodes();
	for (int i=0; i < nl.getLength(); i++) {
	    org.w3c.dom.Node n = nl.item(i);
	    if (!(n instanceof Element))
		continue;
	    content = content + XMLUtils.flatten(n, false);
	}
	setProperty("_result", content);
    }
}



