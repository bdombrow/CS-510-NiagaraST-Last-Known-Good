// $Id: XMLScan.java,v 1.1 2005/08/25 02:24:01 vpapad Exp $

package niagara.logical;

/**
 * A simplified version of FileScan, with a bundled set of unnests.
 */

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

public class XMLScan extends Stream {
    Attrs attrs;
    
    // Required zero-argument constructor
    public XMLScan() {}
    
    public XMLScan(FileScanSpec fileScanSpec, Attribute variable, Attrs attrs) {
        this.streamSpec = fileScanSpec;
        this.variable = variable;
        this.attrs = attrs;
    }

    public void dump() {
        System.out.println("FileScan Operator: ");
        streamSpec.dump(System.out);
        System.out.println("Unnest attributes: " + attrs + "\n");
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        Attrs allAttrs = new Attrs();
        allAttrs.add(variable);
        allAttrs.add(new Variable ("_" + variable.getName() + "_unnest_top"));
        allAttrs.merge(attrs);
        return new LogicalProperty(
            1,
            allAttrs,
            true);
    }
    
    public Op opCopy() {
        return new XMLScan((FileScanSpec) streamSpec, variable, attrs);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof XMLScan)) return false;
        if (obj.getClass() != XMLScan.class) return obj.equals(this);
        XMLScan other = (XMLScan) obj;
        return streamSpec.equals(other.streamSpec) && variable.equals(other.variable) && attrs.equals(other.attrs);
    }

    public int hashCode() {
        return streamSpec.hashCode() ^ variable.hashCode() ^ attrs.hashCode();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        boolean isStream = e.getAttribute("isstream").equalsIgnoreCase("yes");
        streamSpec =
            new FileScanSpec(e.getAttribute("filename"), isStream);
        variable = new Variable(e.getAttribute("id"));
        
        String[] sattrs = e.getAttribute("attrs").split(",");
        if (sattrs.length == 0 || sattrs[sattrs.length-1].length() == 0)
            throw new InvalidPlanException("XMLScan must unnest at least one attribute");
        attrs = new Attrs();
        for (int i = 0; i < sattrs.length; i++) {
            attrs.add(new Variable(sattrs[i]));
        }
    }
    
    public Attrs getAttrs() {
        return attrs;
    }
}


