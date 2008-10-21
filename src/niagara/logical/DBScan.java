// $Id: DBScan.java,v 1.8 2008/10/21 23:11:38 rfernand Exp $

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
	
	private SimilaritySpec sSpec;
	
	private PrefetchSpec pfSpec;
	   
	// Required zero-argument constructor
    public DBScan() {}
    
    public DBScan(DBScanSpec dbScanSpec, Attribute[] variables, SimilaritySpec sSpec, PrefetchSpec pfSpec) {
        this.dbScanSpec = dbScanSpec;
        this.variables = variables;
        this.sSpec = sSpec;
		this.pfSpec = pfSpec;
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
    System.out.println("   "+pfSpec.toString());
	System.out.println();
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(
            1,
            new Attrs(variables, Array.getLength(variables)), true);
    }
    
    public Op opCopy() {
        return new DBScan((DBScanSpec) dbScanSpec, variables, sSpec, pfSpec);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DBScan)) return false;
        if (obj.getClass() != DBScan.class) return obj.equals(this);
        DBScan other = (DBScan) obj;
        return dbScanSpec.equals(other.dbScanSpec) && equalsNullsAllowed(pfSpec, other.pfSpec) &&
        	variables.equals(other.variables) && equalsNullsAllowed(sSpec, other.sSpec);
    }

    public int hashCode() {
        return dbScanSpec.hashCode() ^ variables.hashCode();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
    	String qstring = e.getAttribute("query_string");
    	String[] attrNames = parseInputAttrs(e.getAttribute("attr_names"));
    	String[] attrTypes = parseInputAttrs(e.getAttribute("attr_types"));
    	String timeattr = e.getAttribute("timeattr");
    	String type = e.getAttribute("type");
        dbScanSpec =
            new DBScanSpec(qstring, attrNames, attrTypes, timeattr, type);
        int numAttrs = Array.getLength(attrNames);
        variables = new Attribute[numAttrs];
        for(int i = 0; i< numAttrs; i++) {
        	variables[i] = new Variable(attrNames[i]);	
        }
        
      String similarityStr = e.getAttribute("similarity");
      if (similarityStr != "") {
      	String [] similarityMetric = similarityStr.split("[\t | ]+");
        if(similarityMetric.length != 3 &&
            similarityMetric.length != 4) 
          throw new InvalidPlanException("Invalid similarity string " + similarityStr);
     		sSpec = new SimilaritySpec(similarityMetric[0], 
     				Integer.valueOf(similarityMetric[1]), Integer.valueOf(similarityMetric[2]), false);
     		// if weather is the fourth of our similarity metric
     		if (similarityMetric.length == 4) {
     			if (similarityMetric[3].compareToIgnoreCase("true") == 0)
     				sSpec.setWeather(true);
     		}
      }
      String prefetchStr = e.getAttribute("prefetch");
      if (prefetchStr != "") {
    	  String[] prefetch = prefetchStr.split("[\t| ]+");
    	  if (prefetch.length != 2)
    		  throw new InvalidPlanException("Invalid prefetch string "+prefetchStr);
    	  
    	  pfSpec = new PrefetchSpec(prefetchStr);  
      }
		
    }  
    
    public SimilaritySpec getSimilaritySpec() {
		return sSpec;
    }

    public PrefetchSpec getPrefetchSpec() {
		return pfSpec;
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
