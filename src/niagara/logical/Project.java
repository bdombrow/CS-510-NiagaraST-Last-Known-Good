/* $Id: Project.java,v 1.3 2002/10/31 03:32:21 vpapad Exp $ */
package niagara.logical;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.Strings;
import niagara.xmlql_parser.op_tree.unryOp;

/** Project operator (without duplicate elimination) */
public class Project extends unryOp {
    /** attributes to project on */
    Attrs attrs;
   
    public Op copy() {
        return new Project(this);
    }

    public Project() {}

    public Project(Attrs attrs) {
        this.attrs = attrs;
    }

    public Project(Project other) {
        attrs = other.attrs.copy();
    }

    public void dump() {System.out.println(this);}
    
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Project))
            return false;
        if (other.getClass() != Project.class)
            return other.equals(this);
        return attrs.equals(((Project) other).attrs);
    }
    
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty inputLogProp = input[0];
        assert inputLogProp.Contains(attrs) : "Cannot project on invisible attributes";
        LogicalProperty result = inputLogProp.copy();        
        Attrs oldAttrs = result.getAttrs();
        // Maintain order of input tuple for projected attributes
        Attrs newAttrs = new Attrs(attrs.size());
        for (int i = 0; i < oldAttrs.size(); i++) {
            Attribute a = oldAttrs.get(i);
            if (attrs.Contains(a))
                newAttrs.add(a);
        }
        result.SetAttrs(newAttrs);
        return result;
    }
    
    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return attrs.hashCode();
    }
}
