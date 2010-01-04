/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package fr.imag.adele.teamwork.db.impl;

import static fr.imag.adele.teamwork.db.ModelVersionDBService.HSQL_IN_MEMORY_TYPE;
import static fr.imag.adele.teamwork.db.ModelVersionDBService.HSQL_TYPE;
import static fr.imag.adele.teamwork.db.ModelVersionDBService.MYSQL_TYPE;
import static fr.imag.adele.teamwork.db.ModelVersionDBService.ORACLE_TYPE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;



import fr.imag.adele.teamwork.db.ModelVersionDBService;

public class ConnectionDef {
	
	/*
	 * Logger.
	 */
	private Logger m_logger;
	
	/*
	 * Connection parameters
	 */
	private String m_login;
	private String m_password;
	private String m_baseName;
	private String m_baseType;
	private String m_baseHost;
	private Integer m_basePort;
	private String m_url;
	
	/*
	 * Connection dependent fields
	 */
	private int m_clientIdx;
	private String m_lastSeqName;
	private TypeUtil m_typeUtil;
	private boolean m_supportMultiRequest;
	private int m_tableNotExistErrorCode = -22;
	
	private Connection m_connection;
	
	/*
	 * Transaction Management
	 */
	private String m_savepointPrefix;
	private static AtomicInteger m_lastSavepointNb = new AtomicInteger(0);
	private ThreadLocalSavepointName m_savepoints = new ThreadLocalSavepointName();
	
	public ConnectionDef(String url, String login, String password, Logger logger) {
		m_logger = logger;
		
		m_url = url;
		m_login = login;
		m_password = password;
		m_baseType = getBaseType(url);
		m_baseHost = null;
		m_basePort = null;
		m_baseName = null;
		
		checkConnectionConfig();
	}
	
	public ConnectionDef(String url, Logger logger) {
		this(url, null, null, logger);
	}
	
	public ConnectionDef(String dbType, String host, int port, String dbName,
			String login, String password, Logger logger) {
		m_logger = logger;
		
		m_login = login;
		m_password = password;
		m_baseType = dbType;
		m_baseHost = host;
		m_basePort = port;
		m_baseName = dbName;
		m_url = computeURL();
		
		checkConnectionConfig();
	}

	private String getBaseType(String url) {
		String[] urlParts = url.split(":");
		if (urlParts.length < 2)
			return null;
		
		String jdbcType = urlParts[1];
		String baseType = null;
		if (jdbcType.equals("hsqldb")) {
			if ((urlParts.length > 2) && (urlParts[2].equalsIgnoreCase("mem")))
				baseType = ModelVersionDBService.HSQL_IN_MEMORY_TYPE;
			else
				baseType = ModelVersionDBService.HSQL_TYPE;
		}
		if (jdbcType.equals("mysql")) {
			baseType = ModelVersionDBService.MYSQL_TYPE;
		}
		if (jdbcType.startsWith("oracle")) {
			baseType = ModelVersionDBService.ORACLE_TYPE;
		}
		
		return baseType;
	}
	
	private String computeURL() {
		
		// JDBC URL
		StringBuffer urlSB = new StringBuffer("jdbc:");
		
		if (isHSQL())
			urlSB.append("hsqldb");
		if (isMySQL())
			urlSB.append("mysql");
		if (isOracle())
			urlSB.append("oracle");
		
		urlSB.append(":");
		if (isHSQL() && !isInMemory()) {
			urlSB.append("hsql:");
		} else if (isHSQL() && isInMemory()) {
			urlSB.append("mem:");
		} else if (isOracle()) {
			urlSB.append("thin:@");
		}
		
		if (!isInMemory()) {
			urlSB.append("//");
			
			if (m_baseHost != null)
				urlSB.append(m_baseHost);
			else
				urlSB.append("localhost");
			urlSB.append(":");
			if (m_basePort != null)
				urlSB.append(m_basePort);
			else
				urlSB.append(getDefaultPort());
			
			urlSB.append("/");
		}
		urlSB.append(m_baseName);

		return urlSB.toString();
	}
	
	private void checkConnectionConfig() {
		m_typeUtil = new TypeUtil(m_baseType);
		
		if (isHSQL() && !isInMemory()) {
			m_tableNotExistErrorCode = -22;
			m_supportMultiRequest = true;
			return;
		}
		
		if (isHSQL() && isInMemory()) {
			m_tableNotExistErrorCode = -22;
			m_supportMultiRequest = true;
			return;
		}
		
		if (isMySQL()) {
			m_tableNotExistErrorCode = 1146;
			m_supportMultiRequest = false;
			return;
		}
		
		if (isOracle()) {
			m_tableNotExistErrorCode = 942;
			m_supportMultiRequest = false;
			return;
		}
		
		throw new IllegalStateException("Unsupported database.");
	}
	
	private boolean isInMemory() {
		return HSQL_IN_MEMORY_TYPE.equalsIgnoreCase(m_baseType);
	}

	public String getLogin() {
		return m_login;
	}
	public void setLogin(String login) {
		m_login = login;
	}

	public Connection getConnection() {
		return m_connection;
	}

	public String getPassword() {
		return m_password;
	}

	public void setPassword(String password) {
		m_password = password;
	}

	public String getBaseType() {
		return m_baseType;
	}

	public int getTableNotExistErrorCode() {
		return m_tableNotExistErrorCode;
	}
	
	public boolean isHSQL() {
		return HSQL_TYPE.equalsIgnoreCase(m_baseType) ||
			HSQL_IN_MEMORY_TYPE.equalsIgnoreCase(m_baseType);
	}
	
	public boolean isMySQL() {
		return MYSQL_TYPE.equalsIgnoreCase(m_baseType);
	}

	public boolean isOracle() {
		return ORACLE_TYPE.equalsIgnoreCase(m_baseType);
	}

	public String getURL() {
		return m_url;
	}

	public boolean isClosed() {
		if (m_connection == null)
			return true;
		
		try {
			return m_connection.isClosed();
		} catch (SQLException e) {
			m_logger.log(Level.SEVERE, "Unable to close the connection to database " + m_url, e);
			return true;
		}
	}
	
	private String getDefaultPort() {
		if (isHSQL())
			return "9001";
		
		if (isMySQL())
			return "3306";
		
		if (isOracle())
			return "1521";
		
		return null;
	}
	
	void openConnection() {
		try {
			if ((m_connection == null) || (m_connection.isClosed())) {
				if (m_login == null) {
					m_connection = DriverManager.getConnection(m_url);
					m_logger.log(Level.INFO, "JDBC Connection to " + m_url
							+ " opened without login.");
				} else {
					m_connection = DriverManager.getConnection(m_url, m_login,
							m_password);
					m_logger.log(Level.INFO, "JDBC Connection to " + m_url
							+ " opened with login " + m_login + ".");
				}
				m_connection.setAutoCommit(false);
			}
		} catch (SQLException e) {
			m_logger.log(Level.SEVERE, "Cannot get the connection to database using URL " + 
					m_url + ": " + e.getMessage(), e);
		}
	}

	public void commit() throws SQLException {
		m_connection.commit();
	}

	public TypeUtil getTypeUtil() {
		return m_typeUtil;
	}

	public int getClientIdx() {
		return m_clientIdx;
	}

	public void setClientIdx(int idx) {
		m_clientIdx = idx;
	}

	public boolean isSupportMultiRequest() {
		return m_supportMultiRequest;
	}

	public String getLastSeqName() {
		return m_lastSeqName;
	}

	public void setLastSeqName(String seqName) {
		m_lastSeqName = seqName;
	}

	public void close() {
		if (isClosed())
			return;
		
		try {
			m_connection.close();
		} catch (SQLException e) {
			m_logger.log(Level.SEVERE, "[ERROR IN DATABASE ACCESS] Cannot close the connection : "
					+ e.getMessage(), e);
		}
	}
	
	public void resetSavepoints() {
		m_lastSavepointNb = new AtomicInteger(0);
	}

	public String getSavepointPrefix() {
		return m_savepointPrefix;
	}

	public void setSavepointPrefix(String prefix) {
		m_savepointPrefix = prefix;
	}

	public void createNewSavepoint() {
		// Savepoint name should be unique globally, ie. for all JDBC clients
		try {
			String savepointName = getSavepointPrefix() + m_lastSavepointNb.incrementAndGet();
			Savepoint savepoint = m_connection.setSavepoint(savepointName);
			m_savepoints.newSavepoint(savepoint);
		} catch (SQLException e) {
			m_logger.log(Level.SEVERE, "Set savepoint failed : ", e);
		} 
	}
	
	public void rollback() throws SQLException {
		if (isClosed())
			return;
		
		m_connection.rollback();
	}
	
	@Override
	public String toString() {
		return m_url;
	}

	public Savepoint popLastSavepoint() {
		return m_savepoints.popLastSavepoint();
	}
}
