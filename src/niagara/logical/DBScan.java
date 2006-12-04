// $Id: DBScan.java,v 1.1 2006/12/04 21:15:40 tufte Exp $

package niagara.logical;

/**
 * This class represents stream Scan operator that fetches data
 * from a stream, parses it and returns the DOM tree to the operator
 * above it. 
 */

import org.w3c.dom.Element;

import java.lang.reflect.Array;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

public class DBScan extends NullaryOperator  {
	/** the attribute we create */
	/** todo: needs to create multiple attributes */
	private Attribute[] variables;
	private DBScanSpec dbScanSpec;
	   
	// Required zero-argument constructor
    public DBScan() {}
    
    public DBScan(DBScanSpec dbScanSpec, Attribute[] variables) {
        this.dbScanSpec = dbScanSpec;
        this.variables = variables;
    }
    
    /**
     * Returns the specification for this stream scan
     *
     * @return The specification for this stream as a FileSpec
     *         object
     */

    public void dump() {
	System.out.println("DBScan Operator: ");
	dbScanSpec.dump(System.out);
	System.out.println();
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(
            1,
            new Attrs(variables, Array.getLength(variables)), true);
    }
    
    public Op opCopy() {
        return new DBScan((DBScanSpec) dbScanSpec, variables);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DBScan)) return false;
        if (obj.getClass() != DBScan.class) return obj.equals(this);
        DBScan other = (DBScan) obj;
        return dbScanSpec.equals(other.dbScanSpec) && 
        	variables.equals(other.variables);
    }

    public int hashCode() {
        return dbScanSpec.hashCode() ^ variables.hashCode();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
    	String qstring = e.getAttribute("query_string");
    	String[] attrNames = parseInputAttrs(e.getAttribute("attr_names"));
    	String[] attrTypes = parseInputAttrs(e.getAttribute("attr_types"));
        dbScanSpec =
            new DBScanSpec(qstring, attrNames, attrTypes);
        int numAttrs = Array.getLength(attrNames);
        variables = new Attribute[numAttrs];
        for(int i = 0; i< numAttrs; i++) {
        	variables[i] = new Variable(attrNames[i]);	
        }
    }  
    
    public DBScanSpec getSpec() {
        return dbScanSpec;
    }

    public Attribute[] getVariables() {
        return variables;
    }
    
    public boolean isSourceOp() {
        return true;
    }
    
    
}
