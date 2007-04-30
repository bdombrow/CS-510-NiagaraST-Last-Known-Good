/**
 * $Id: ShowTunables.java,v 1.1 2007/04/30 19:21:17 vpapad Exp $
 *
 */

package niagara.logical;

/**
 * ShowTunables is a pseudo-operator to show the tunable parameters
 * in a plan of operators
 */

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class ShowTunables extends NullaryOperator {
    private String planID;

    private Attrs attrs;

    public ShowTunables() {
        attrs = getDefaultAttrs();
    }

    public ShowTunables(String planID) {
        this();
        this.planID = planID;
    }

    public String getPlanID() {
        return planID;
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" plan='").append(planID).append("'");
    }

    public boolean isSourceOp() {
        return true;
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(1, attrs, true);
    }

    public Op opCopy() {
        return new ShowTunables(planID);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ShowTunables))
            return false;
        if (obj.getClass() != ShowTunables.class)
            return obj.equals(this);
        ShowTunables other = (ShowTunables) obj;
        return planID.equals(other.planID);
    }

    public int hashCode() {
        return planID.hashCode();
    }

    public Attrs getAttrs() {
        return attrs;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties,
            Catalog catalog) throws InvalidPlanException {
        planID = e.getAttribute("plan");
    }

    public static Attrs getDefaultAttrs() {
        Attrs attrs = new Attrs();
        attrs.add(new Variable("operator"));
        attrs.add(new Variable("name"));
        attrs.add(new Variable("type"));
        attrs.add(new Variable("description"));
        attrs.add(new Variable("value"));
        return attrs;
    }
}
