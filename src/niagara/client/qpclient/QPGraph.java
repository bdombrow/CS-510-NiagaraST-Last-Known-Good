package niagara.client.qpclient;
import diva.graph.model.Node;
import diva.graph.model.Graph;
import diva.util.SemanticObjectContainer;
import diva.util.PropertyContainer;
import java.util.Iterator;
import diva.util.BasicPropertyContainer;
import java.util.Iterator;
import java.util.ArrayList;


public class QPGraph implements Graph  {
    ArrayList nodes;

    public QPGraph() {
	nodes = new ArrayList();
    }


    public void add(Node n) {
	nodes.add(n);
    }

    public void remove(Node n) {
	nodes.remove(nodes.indexOf(n));
    }

    public boolean contains(Node n) {
	return nodes.contains(n);
    }

    public int getNodeCount() {
	return nodes.size();
    }

    public Iterator nodes() {
	return nodes.iterator();
    }

    BasicPropertyContainer bpc = new BasicPropertyContainer();

    public Object getProperty(String param1) {
	return bpc.getProperty(param1);
    }
        
    public void setProperty(String param1, Object param2) {
	bpc.setProperty(param1, param2);
    }
    private Object semanticObject;
    
    public Object getSemanticObject() {
	return semanticObject;
    }

    public void setSemanticObject(Object semanticObject) {
	this.semanticObject = semanticObject;
    }


}


