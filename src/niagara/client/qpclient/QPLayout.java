package niagara.client.qpclient;

import diva.graph.layout.*;
import diva.graph.model.*;
import java.util.Iterator;
import java.awt.geom.Rectangle2D;

/**
 * <code>QPLayout</code> is a global layout that places nodes at the positions
 * specified by their respective DOM element.
 * Adapted from RandomLayout, by Michael Shilman.
 *
 * @see GlobalLayout
 */

public class QPLayout implements GlobalLayout {
    public void layout(LayoutTarget target, Graph g) {
	Rectangle2D vp = target.getViewport(g);
        for(Iterator ns = g.nodes(); ns.hasNext(); ) {
            Node n = (Node)ns.next();
	    QPNode qpn = (QPNode) n;
	    String xattr = qpn.getDOMElement().getAttribute("x");
	    String yattr = qpn.getDOMElement().getAttribute("y");
	    double x, y;

	    if (!(xattr.equals("") || yattr.equals(""))) {
		x  = Double.parseDouble(xattr);
		y  = Double.parseDouble(yattr);
	    }
	    else {
		x = Math.random() * vp.getWidth(); 
		y = Math.random() * vp.getHeight();
	    }

	    LayoutUtilities.place(target, n, x, y);
        }
    }
}
