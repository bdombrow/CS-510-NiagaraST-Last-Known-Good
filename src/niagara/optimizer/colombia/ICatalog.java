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
