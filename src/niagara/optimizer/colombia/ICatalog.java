/* $Id: ICatalog.java,v 1.2 2003/02/08 02:12:03 vpapad Exp $ */
package niagara.optimizer.colombia;

/**
 * The interface Colombia expects from the database's catalog
 */
public interface ICatalog {
    // Cost model parameters
    double getDouble(String parameter);
    int getInt(String parameter);
    
    LogicalProperty getLogProp(String collectionName);
}
