package niagara.client.qpclient;

import diva.graph.model.*;

public class QPGraphImpl implements GraphImpl {
    
    public void addNode(Node n, Graph parent) {
	((QPGraph) parent).add(n);
	((QPNode) n).setParent(parent);
    }
    
    public CompositeNode createCompositeNode(Object param1) {
	System.err.println("Composite nodes not supported yet");
	return null;
    }
    
    public Edge createEdge(Object o) {
	QPEdge be = new QPEdge();
	be.setSemanticObject(o);
	return be;
    }
    
    public Graph createGraph(Object o) {
	QPGraph qpg = new QPGraph();
	qpg.setSemanticObject(o);
	return qpg;
    }
    
    public Node createNode(Object o) {
	System.err.println("You have to create a specific node type");
	return null;
    }
    
    public Node createScan(Object o) {
	Node scan = new Scan();
	scan.setSemanticObject(o);
	return scan;
    }

    public void removeNode(Node n) {
	((QPGraph) n.getParent()).remove(n);
    }
    

    public void setEdgeHead(Edge e, Node n) {
	((QPEdge) e).setHead((QPNode) n);	
	((QPNode) n).becomesHead(e);
    }
    
    public void setEdgeTail(Edge e, Node n) {
	((QPEdge) e).setTail((QPNode) n);
	((QPNode) n).becomesTail(e);
    }
    
}



