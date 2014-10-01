package org.siforge.sm;

/** Questa classe modella un generico errore di sincronizzazione.
 * Va sempre giustificato l'errore con un messaggio appropiato
 * E' una sottoclasse
 * di SQLException, in modo che sia facile da rubricare come "errore di db"
 * in codice legacy.
 * @author  Gio
 */
public class SyncException extends java.sql.SQLException {

    /**
     * Constructs an instance of <code>SyncException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public SyncException(String msg) {
        super(msg);
    }
}
