/**
 * $Id: InsufficientMemoryException.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

/**
 *
 * BufferManager preallocates and maintains an array of pages of SAX events,
 * and manages their translation into DOM events.
 *
 */

public class InsufficientMemoryException extends RuntimeException {
}
