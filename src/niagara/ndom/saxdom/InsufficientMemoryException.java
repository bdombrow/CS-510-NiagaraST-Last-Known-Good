package niagara.ndom.saxdom;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 * BufferManager preallocates and maintains an array of pages of SAX events, and
 * manages their translation into DOM events.
 * 
 */

@SuppressWarnings("serial")
public class InsufficientMemoryException extends RuntimeException {
}
