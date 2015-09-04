package org.siforge.sm.pump;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.siforge.sm.SyncException;

/**
 * Ideal for backup: you can easily bring a source database in a sqlite-embedded database out
 * @author Giorgig
 *
 */
public class SQLitePump extends SmartSyncPump {

	@Override
	void createTable(String tableName, Connection destConnection,
			ResultSetMetaData metaData, int... jdbcTypes) 
	throws SQLException {
		String sql="Create table "+tableName+" ( ";
		for(int i=1; i<=metaData.getColumnCount(); i++){
			sql+=" "+metaData.getColumnName(i);
			sql+=" string";
			if(i != metaData.getColumnCount()){
				sql+=",";
			}
		}
		sql+=")";
		logger.trace("Generic...."+ sql);
		int n=destConnection.prepareStatement(sql).executeUpdate();
//		if(n!=1){
//			throw new SyncException("Cannot create table");
//		}
	}

	

}
