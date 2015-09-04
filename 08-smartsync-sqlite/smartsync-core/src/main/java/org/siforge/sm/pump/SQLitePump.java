package org.siforge.sm.pump;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
		destConnection.prepareStatement(sql).execute();
	}

	

}
