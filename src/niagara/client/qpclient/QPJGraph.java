package niagara.client.qpclient;

import java.awt.*;
import diva.graph.*;
import diva.graph.model.*;

public class QPJGraph extends JGraph {
    public QPJGraph() {
	super();
    }

    public QPJGraph(GraphModel gm) {
	super(gm);
    }
    
    public void paint(Graphics g) {
	// Catch exceptions from JCanvas...
	try {
	    super.paint(g);
	}
	catch (Exception e) {
	}
    }
}
