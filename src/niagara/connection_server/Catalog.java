/*
 * $Id: Catalog.java,v 1.5 2002/05/23 06:30:47 vpapad Exp $
 *
 */

package niagara.connection_server;
import java.util.*;
import java.io.*;

import org.w3c.dom.*;
import org.xml.sax.InputSource;

import niagara.ndom.*;
import niagara.utils.*;

public class Catalog {

    private static final boolean debug = true;

    // This primitive Catalog maintains a mapping from URNs to URLs, 
    // or location of servers that know to resolve the URNs
    
    Hashtable urn2urls; // locally resolvable
    Hashtable urn2resolvers;

    HashMap costmodel; // Columbia cost model parameters

    // Logical operator name <-> class mapping
    HashMap name2class; 
    HashMap class2name;

    // Logical operator -> Array of Physical operator classes
    HashMap logical2physical;

    public Catalog() {
        urn2urls = new Hashtable();
        urn2resolvers = new Hashtable();
        costmodel = new HashMap();
        name2class = new HashMap();
        class2name = new HashMap();
        logical2physical = new HashMap();
    }

    public void addResolver(String urn, String resolver) {
        if (!urn2resolvers.containsKey(urn)) {
            urn2resolvers.put(urn, new Vector());
        }
        Vector resolvers = (Vector) urn2resolvers.get(urn);
        resolvers.add(resolver);
    }

    public void addURL(String urn, String url) {
        if (!urn2urls.containsKey(urn)) {
            urn2urls.put(urn, new Vector());
        }
        Vector urls = (Vector) urn2urls.get(urn);
        urls.add(url);
    }

    public boolean isLocallyResolvable(String urn) {
        return urn2urls.containsKey(urn);
    }

    public Vector getURL(String urn) {
        return (Vector) urn2urls.get(urn);
    }

    public Vector getResolvers(String urn) {
        return (Vector) urn2resolvers.get(urn);
    }

    public Hashtable getResolvers(Vector urns) {
        Hashtable resolver2urns = new Hashtable();
        for (int i = 0; i < urns.size(); i++) {
            String urn = (String) urns.get(i);
            Vector resolvers = (Vector) urn2resolvers.get(urn);
            if (resolvers == null) continue;
            for (int j = 0; j < resolvers.size(); j++) {
                String resolver = (String) resolvers.get(j);
                if (!resolver2urns.containsKey(resolver)) {
                    resolver2urns.put(resolver, new Vector());
                }
                Vector resolvable = (Vector) resolver2urns.get(resolver);
                resolvable.add(urn);
            }
        }
        return resolver2urns;
    }

    public Catalog(String filename) {
        this();

        // parse the catalog file
        niagara.ndom.DOMParser parser = DOMFactory.newParser();

        InputSource is = null;
        try {
            is = new InputSource(new FileInputStream(filename));
            parser.parse(is);

            // Get document
            Document d = parser.getDocument();
            
            // Get root element
            Element root = d.getDocumentElement();

            loadOperators(root);
            loadResources(root);
            loadCostModel(root);
        }
        catch (FileNotFoundException e) {
            throw new PEException("Catalog file not found: " + e.getMessage());
	} catch(org.xml.sax.SAXException se) {
	    throw new PEException("Error parsing catalog file " 
                                  + se.getMessage());
	} catch(IOException ioe) {
	    throw new PEException("Error reading catalog file " +
				  ioe.getMessage());
	}
    }
    
    void loadOperators(Element root) {
        costmodel = new HashMap();
        NodeList nl = root.getElementsByTagName("operators");
        if (nl.getLength() != 1) {
            throw new PEException(
                 "The catalog must contain exactly one <operators> element");
        }
        Node cm = nl.item(0);

        nl = cm.getChildNodes(); 

        // Populate the operator hashmaps
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            Element e = (Element) n;
            String name = e.getAttribute("name");
            String className = e.getAttribute("class");
            name2class.put(name, className);
            name2class.put(className, name);

            ArrayList al = new ArrayList();
            NodeList physical = e.getChildNodes();
            for (int j = 0; j < physical.getLength(); j++) {
                Node m = physical.item(j);
                if (m.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                 Element elt = (Element) m;
                 String physicalClassName = elt.getAttribute("class");
                 try {
                     al.add(Class.forName(physicalClassName));
                 }
                 catch (ClassNotFoundException exc) {
                     throw new PEException("Could not find class: " 
                                           + physicalClassName);
                 }
            }
            Class[] implementations = new Class[al.size()];
            for (int k = 0; k < al.size(); k++)
                implementations[k] = (Class) al.get(k);
            logical2physical.put(className, implementations);
        }
    }

    void loadResources(Element root) {
        NodeList resources = root.getElementsByTagName("resource");
        for (int i = 0; i < resources.getLength(); i++) {
            Element resource = (Element) resources.item(i);
            String urn = resource.getAttribute("name");
            
            NodeList urls = resource.getElementsByTagName("url");
            for (int j = 0; j < urls.getLength(); j++) {
                Element url = (Element) urls.item(j);
                String location = url.getAttribute("location");
                if (!urn2urls.containsKey(urn)) { 
                    urn2urls.put(urn, new Vector());
                }
                Vector v = (Vector) urn2urls.get(urn);
                v.add(location);
            }

            NodeList resolvers = resource.getElementsByTagName("resolver");
            for (int j = 0; j < resolvers.getLength(); j++) {
                Element resolver = (Element) resolvers.item(j);
                String location = resolver.getAttribute("location");
                if (!urn2resolvers.containsKey(urn)) {
                    urn2resolvers.put(urn, new Vector());
                }
                Vector v = (Vector) urn2resolvers.get(urn);
                v.add(location);
            }
        }
    }

    void loadCostModel(Element root) {
        costmodel = new HashMap();
        NodeList nl = root.getElementsByTagName("costmodel");
        if (nl.getLength() != 1) {
            throw new PEException(
                 "The catalog must contain exactly one <costmodel> element");
        }
        Node cm = nl.item(0);

        nl = cm.getChildNodes(); 

        // Populate the cost model HashMap
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE)
                continue;
            Element e = (Element) n;
            costmodel.put(e.getTagName(), e.getAttribute("value"));
        }
    }

    
    public String getOperatorName(String operatorClass) {
        return (String) class2name.get(operatorClass);
    }

    public Class[] getPhysical(String logicalClass) { 
        return (Class[]) logical2physical.get(logicalClass);
    }

    public String getCostModelParameter(String param) {
        return (String) costmodel.get(param);
    }

    public int getIntegerCostModelParameter(String param) {
        String s = (String) costmodel.get(param);
        return Integer.parseInt(s);
    }

    public double getDoubleCostModelParameter(String param) {
        String s = (String) costmodel.get(param);
        return Double.parseDouble(s);
    }

    public static void cerr(String msg) {
        System.err.println(msg);
    }
    
    // for testing 
    public void dumpStats() {
        cerr("Locally resolvable URNs:");
        Enumeration e = urn2urls.keys();
        while (e.hasMoreElements()) {
            String urn = (String) e.nextElement();
            cerr("\t" + urn + ":");
            Vector urls = (Vector) urn2urls.get(urn);
            for (int i = 0; i < urls.size(); i++) {
                cerr("\t\t" + urls.get(i));
            }
        }
        Vector allResolvableURNs = new Vector();
        e = urn2resolvers.keys();
        while (e.hasMoreElements()) {
            String urn = (String) e.nextElement();
            allResolvableURNs.add(urn);
        }
        
        Hashtable resolver2urns = getResolvers(allResolvableURNs);

        cerr("Resolvers for URNs:");
        Enumeration resolvers = resolver2urns.keys();
        while (resolvers.hasMoreElements()) {
            String resolver = (String) resolvers.nextElement();
            cerr(resolver + ": ");
            Vector resolvables = (Vector) resolver2urns.get(resolver);
            for (int i = 0; i < resolvables.size(); i++) {
                cerr("\t" + resolvables.get(i));
            }
        }
    }

    public void dumpCostModel() {
        cerr("Cost model parameters: ");
        Iterator keys = costmodel.keySet().iterator();
        while (keys.hasNext()) {
            String k = (String) keys.next();
            cerr(k + " = " + costmodel.get(k));
        }
    }
}

