package niagara.client.qpclient;

import java.awt.Graphics;

import diva.graph.JGraph;
import diva.graph.model.GraphModel;

@SuppressWarnings("serial")
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
		} catch (Exception e) {
		}
	}
}
