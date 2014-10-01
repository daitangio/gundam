package org.siforge.sm;

/** Questa classe modella un generico errore di sincronizzazione.
 * Va sempre giustificato l'errore con un messaggio appropiato
 *
 * @author  Gio
 */
public class SyncException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
     * Constructs an instance of <code>SyncException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public SyncException(String msg) {
        super(msg);
    }
}
