package niagara.optimizer.colombia;

/**
 * A tracer object handles tracing events from the optimizer
 */
public interface Tracer {
    /** About to create a new group with this multiexpression */
    void beforeNewGroup(MExpr mexpr);
    
    /** A new group was created */
    void afterNewGroup(Group group);
    
    /** A new task is scheduled */
    void addingTask(Task task);
    
    /** A task is about to be performed*/
    void performingTask(Task task);
}
