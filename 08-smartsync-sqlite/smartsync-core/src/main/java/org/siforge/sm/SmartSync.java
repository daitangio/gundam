package org.siforge.sm;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.log4j.Category;
// import javax.servlet.http.*;
import org.apache.log4j.Logger;


/** SmartSync sincronizza il contenuto di una tabella tra due connessioni JDBC.
 * Verifica che i tipi delle colonne siano gli stessi (ma ignora sempre i loro nomi)
 * BUG: Non dovrebbe funzionare con i tipi custom come i  BLOB/CLOB/LOB di Oracle.
 * @author Giovanni Giorgi
 */
public class SmartSync {
    
    /** Log dell'istanza */    
    private Logger   log;
    Connection source,dest;
    int columns=0;
    String targetTable;
    String universalSelect, universalInsert;
    int srcColType[], destColType[];
    
    /**
     * @param tableName
     * @param srcCon
     * @param destCon
     * @param logz
     * @throws SQLException
     */    
    public SmartSync(String tableName, Connection srcCon, Connection destCon, Category logz) throws SQLException {
        this.source=srcCon;
        this.dest=destCon;

        this.log = Logger.getLogger(logz.getName()+".SmartSync."+tableName);
        
        this.targetTable=tableName;
        this.universalSelect="SELECT * FROM "+ this.targetTable;                
        
        ResultSet rs=source.createStatement().executeQuery(universalSelect);        
        
        if(!rs.next()) {
            log.warn("Src Table is empty");
        }
        
        ResultSetMetaData md=rs.getMetaData();
        columns=md.getColumnCount();
        
        
        // Ottiene i tipi sorgente:
        log.info(this.targetTable+" <Source> COLUMNS: "+md.getColumnCount());
        this.srcColType=getTypes(md);
        
        
        
        // Fa lo stesso con la destinzione:
        
        
        ResultSet rdest=dest.createStatement().executeQuery(universalSelect);
        if(rdest.next()){
            log.error("<Dest> Table isn't empty: Sync can fail!");
        }
        
        ResultSetMetaData mdDest=rdest.getMetaData();
        log.info(this.targetTable+" <Dest>   COLUMNS: "+mdDest.getColumnCount());
        this.destColType=getTypes(mdDest);
        
        rs.close(); rdest.close();
        checkDestTypes();
        
        this.universalInsert="INSERT INTO "+this.targetTable +" VALUES (";
        int i=columns;
        while(i>1){
            this.universalInsert += "?,";
            i--;
        }
        this.universalInsert += " ?)";
        //log.debug(this.universalInsert);
        
    }
    
    /** Metodo da chiamare per effettuare una sincronizzazione completa
     */
    public void syncAll() throws SQLException {
        log.debug("SmartSync->"+targetTable);
        // Carico e scarico!
        // Ora inizia il ciclo:
        int type, rowProcessed=0;
        Object objC;
        ResultSet rsSrc= this.source.createStatement().executeQuery(universalSelect);
        PreparedStatement insertStm=this.dest.prepareStatement(universalInsert);
        while(rsSrc.next()){
            
            // Read from Src and write to dest....
            for(int i=1; i<= this.columns; i++){
                objC=rsSrc.getObject(i);
                type=findType(i);
                
                //log.debug("Processing COL:"+i+" Src:"+objC);
                if(objC!=null) {
                    insertStm.setObject(i,objC, type);
                }else{
                    // Gli oggetti null vanno trattati diversamente.
                    insertStm.setNull(i, type);
                }
            }
            insertStm.execute();
            rowProcessed++;
        }
        
        log.debug("SmartSync->"+targetTable+" Rows Processed:"+rowProcessed);
    }
    
    int findType(int pos){
        return this.srcColType[pos-1];
        
    }
    
    
    /**
     * @param type
     * @return
     */    
    private String typeToString(int type) {
        switch(type) {
            case java.sql.Types.ARRAY:return "ARRAY";
            case java.sql.Types.BIGINT: return "BIGINT";
            case java.sql.Types.BINARY: return "BINARY";
            case java.sql.Types.BIT: return "BIT";
            case java.sql.Types.BLOB: return "BLOB";
            case java.sql.Types.CHAR: return "CHAR";
            case java.sql.Types.CLOB: return "CLOB";
            case java.sql.Types.DATE: return "DATE";
            case java.sql.Types.DECIMAL: return "DECIMAL";
            case java.sql.Types.DISTINCT: return "DISTINCT";
            case java.sql.Types.DOUBLE: return "DOUBLE";
            case java.sql.Types.FLOAT: return "FLOAT";
            case java.sql.Types.INTEGER: return "INTEGER";
            case java.sql.Types.JAVA_OBJECT: return "JAVA_OBJECT";
            case java.sql.Types.LONGVARBINARY: return "LONGVARBINARY";
            case java.sql.Types.LONGVARCHAR: return "LONGVARCHAR";
            case java.sql.Types.NULL: return "NULL";
            case java.sql.Types.NUMERIC: return "NUMERIC";
            case java.sql.Types.OTHER: return "OTHER";
            case java.sql.Types.REAL: return "REAL";
            case java.sql.Types.REF: return "REF";
            case java.sql.Types.SMALLINT: return "SMALLINT";
            case java.sql.Types.STRUCT: return "STRUCT";
            case java.sql.Types.TIME: return "TIME";
            case java.sql.Types.TIMESTAMP: return "TIMESTAMP";
            case java.sql.Types.TINYINT: return "TINYINT";
            case java.sql.Types.VARBINARY: return "VARBINARY";
            case java.sql.Types.VARCHAR : return "VARCHAR";
            default:
                return"UNKNOWN_SQL_TYPE?";
        } // switch
    }
    
    /** Controlla che i tipi della tabella di destinazione siano COINCIDENTI
     *  Con quelli di sorgente
     */
    void checkDestTypes() throws SyncException {
        //Confronta srcColType[], destColType[]
        // UNIMPLEMENTED
        if(!java.util.Arrays.equals(srcColType,destColType)){
            log.fatal("Tables Structures do not match!!");
            throw new SyncException("Tables Structures do not match");
        }
    }
    
    /** Questa funzione ritorna i tipi delle colonne  del ResultSetMetaData
     * fornito.
     * Stampa nel log i tipi, in formato comprensibile
     */
    int [] getTypes(ResultSetMetaData md) throws SQLException {
        int colType[];
        int cols=md.getColumnCount();
        
        colType=new int[cols];
        int t=0;
        String msg="  Types:";
        while(t<cols){
            // Rec: le pos partono da 1, gli array da zero!
            colType[t]=md.getColumnType(t+1);
            msg+=typeToString(colType[t]) + " ";
            t++;
        }
        
        log.debug(msg);
        return colType;
    }
    
    
    
    /**
     * @param args
     */    
    public static void main(String[] args)  {
        
        
        
        Connection src=null, dest=null;
        Logger mlog=Logger.getLogger("dbaccess.SmartSyncMain.Main");
        
        try{
            mlog.debug("Getting Connections");
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            src  = DriverManager.getConnection("jdbc:mysql://localhost/dbapp?user=root&password=");
            dest = DriverManager.getConnection("jdbc:mysql://localhost/dbfru?user=root&password=");
            
            
        
            /** Esempio di processing di piu' tabelle.
              * Vengono elencate in ordine di cancellazione, l'ordine 
              * cioe' che consente di cancellare le tabelle senza incorrere
              * in violazioni dei vincoli di integrita'
              * L'ordine di popolamento e' esattamente l'opposto */
            String newsTables[]={"DOCUMENTI", "UTENTI"};
            processTables(newsTables,src,dest,mlog);                     
            dest.commit();
            
        }catch(Throwable e){
            mlog.error("Cannot proceed",e);
            try{
                src.rollback();
                dest.rollback();
            }catch(SQLException e2){
                mlog.error("Cannot Rollback: a very BAD DAY eh?",e2);
            };
        }
    }
    
    

    public static void processTables(String t[], Connection src, Connection dest,
    Logger log) throws SQLException {
    	processTables(t, src, dest, log, false);
    }
    
    /** Fornire le tabelle int ordine di CANCELLAZIONE!
     * @param t
     * @param src
     * @param dest
     * @param log
     * @throws SQLException
     */
    public static void processTables(String t[], Connection src, Connection dest,
    Logger log, boolean delete) throws SQLException {
        // Prima esegue il big del:
    	if(delete){
    		log.info("Requested deletation of target db....");
	        for(int i=0; i<t.length; i++){
	            log.info("Deleting "+t[i]);
	            dest.createStatement().execute("DELETE FROM "+t[i]);
	        }
    	}        
        SmartSync sm;
        
        for(int i=t.length-1; i>=0; i--){
            sm=new SmartSync(t[i], src, dest,log);
            sm.syncAll();
        };
    }
}
