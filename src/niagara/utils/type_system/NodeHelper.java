package niagara.utils.type_system;

/**
 * Interface for classes which will help give this system a very
 * limted form of typing.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


import niagara.utils.nitree.*;

public interface NodeHelper {
    public boolean nodeEquals(NINode lNode, NINode rNode);

    public Class getNodeClass();

    /* converts a Node into an object of the appropriate type */
    public Object valueOf(NINode node);

    public String getName();
}






