package com.sm.fastda.db;

import javax.sql.DataSource;

public class MySQLEngine extends BasicRDBEngine {
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
