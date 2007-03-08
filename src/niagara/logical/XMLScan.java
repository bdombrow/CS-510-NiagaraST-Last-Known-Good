// $Id: XMLScan.java,v 1.4 2007/03/08 22:34:03 tufte Exp $

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
    /** The attributes we're projecting on (null means keep all attributes) */
    Attrs projectedAttrs;

    // Required zero-argument constructor
    public XMLScan() {}
    
    public XMLScan(FileScanSpec fileScanSpec, Attribute variable, Attrs attrs, Attrs projectedAttrs) {
        this.streamSpec = fileScanSpec;
        this.variable = variable;
        this.attrs = attrs;
	this.projectedAttrs = projectedAttrs;
    }

    public void dump() {
        System.out.println("FileScan Operator: ");
        streamSpec.dump(System.out);
        System.out.println("Unnest attributes: " + attrs + "\n");
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
	Attrs allAttrs;
        if (projectedAttrs == null) {
	    allAttrs = new Attrs();
	    allAttrs.add(variable);
	    allAttrs.add(new Variable ("_" + variable.getName() + "_unnest_top"));
	    allAttrs.merge(attrs);
	} else
	    allAttrs = projectedAttrs;

        return new LogicalProperty(
            1,
            allAttrs,
            true);
    }
    
    public void projectedOutputAttributes(Attrs outputAttrs) {
        projectedAttrs = outputAttrs.copy();
    }

    public Attrs getProjectedAttrs() {
	return projectedAttrs;
    }

    public Op opCopy() {
        return new XMLScan((FileScanSpec) streamSpec, variable, attrs, projectedAttrs);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof XMLScan)) return false;
        if (obj.getClass() != XMLScan.class) return obj.equals(this);
        XMLScan other = (XMLScan) obj;
        return streamSpec.equals(other.streamSpec) && variable.equals(other.variable) && attrs.equals(other.attrs) && equalsNullsAllowed(projectedAttrs, other.projectedAttrs);
    }

    public int hashCode() {
        return streamSpec.hashCode() ^ variable.hashCode() ^ attrs.hashCode() ^ hashCodeNullsAllowed(projectedAttrs);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        boolean isStream = e.getAttribute("isstream").equalsIgnoreCase("yes");
				int delay = Integer.getInteger(e.getAttribute("delay")).intValue();
        if(!isStream && delay > 0) {
            throw new InvalidPlanException("delay > 0 allowed only if isstream is yes");
				}
				
        streamSpec =
            new FileScanSpec(e.getAttribute("filename"), isStream, delay);
        variable = new Variable(e.getAttribute("id"));
       
        String[] sattrs = e.getAttribute("attrs").split("\\W+");
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


