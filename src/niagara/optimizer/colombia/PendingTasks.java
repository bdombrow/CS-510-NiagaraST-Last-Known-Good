/* $Id: PendingTasks.java,v 1.2 2003/02/08 02:12:04 vpapad Exp $ */
package niagara.optimizer.colombia;

/**
PendingTasks is a collection of undone tasks is currently stored as a stack.
Other structures are certainly appropriate, but in any case dependencies
must be stored.  For example, a directed graph could be used to
parallelize optimization.
*/
public class PendingTasks {

    private Task first; // anchor of PendingTasks stack

    boolean empty() {
        return (first == null);
    } 

    void push(Task task) {
        task.next = first;
        first = task; //Push Task
    }

    Task pop() {
        if (empty())
            return null;

        Task task = first;
        first = task.next;
        return task;
    }
    
    public void clear() {
        first = null;
    }
}
