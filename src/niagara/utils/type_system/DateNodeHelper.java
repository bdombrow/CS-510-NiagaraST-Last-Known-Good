package niagara.utils.type_system;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import java.text.*;
import java.util.Date;

import niagara.utils.PEException;
import niagara.utils.nitree.*;

/**
 * Class <code> DateNodeHelper </code> which performs functions
 * on two nodes, interpreting the values as dates.
 *
 * Intended to add a "date type" to the niagara system.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


public class DateNodeHelper extends NumberNodeHelperBase
    implements NumberNodeHelper {

    /* for now - we all use a default date format */
    private final static DateFormat dateFormat = new SimpleDateFormat();
    private Class myClass;

    public DateNodeHelper() {
	try {
	    myClass = Class.forName("java.text.DateFormat");
	} catch (ClassNotFoundException e) {
	    throw new PEException(); /* should never get here */
	}
    }

    public Class getNodeClass() { return myClass;}
    
    public Object valueOf(NINode node) {
	/* don't really know what we should do for error checking
	 * here - just do this for now until I figure out something
	 * reasonable.  This is ugly!! 
	 */
	try {
	    return dateFormat.parse(node.myGetNodeValue());
	} catch (ParseException e) {
	    throw new PEException("ParseException: " + e.getMessage());
	}
    }

    public boolean nodeEquals(NINode lNode, NINode rNode) {	
	return valueOf(lNode).equals(valueOf(rNode));
    }

    public boolean lessThan(NINode lNode, NINode rNode) {
	return ((Date)valueOf(lNode)).before((Date)valueOf(rNode));
    }

    public boolean average(NINode lNode, NINode rNode, 
			   NINode resultNode) {
	throw new PEException("Average called on Date !!");
    }

    public boolean sum(NINode lNode, NINode rNode, NINode resultNode) 
	throws NITreeException {
	long lVal = ((Date)valueOf(lNode)).getTime();
	long rVal = ((Date)valueOf(rNode)).getTime();
	Date d = new Date(lVal + rVal);
	resultNode.mySetNodeValue(d.toString());
	return true;
    }

    public String getName() {
        return "DateNodeHelper";
    }

    public String toString() {
        return getName();
    }
}
