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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.felix.ipojo.util.Logger;
import org.osgi.framework.BundleContext;


import fr.imag.adele.teamwork.db.ModelVersionDBException;
import fr.imag.adele.teamwork.db.ModelVersionDBService;
import fr.imag.adele.teamwork.db.Revision;
import fr.imag.adele.teamwork.db.TransactionException;
import static fr.imag.adele.teamwork.db.impl.TableConstants.*;

/**
 * This model version database service implementation uses a HSQL, MySQL or Oracle database. 
 * HSQL 1.8.x transactions don't support all kind of concurrent accesses
 * (all clients see the modified values during transactions).
 * If you use HSQL 1.9 (except READ_COMMITED) or 2.x, transactions support all kind of isolation levels.
 * 
 * Comments: - There MUST NOT be a same id for a link type and an object type.
 * It is your responsibility of ensuring type id unicity between objects and
 * links.
 * 
 * This implementation is not Thread safe.
 * 
 * Revision numbers is the sequence of integers from 1. 
 * Successor(rev) = rev + 1;
 * Predecessor(rev) = rev - 1.
 * 
 * @author Leveque Thomas
 * 
 */
public class ModelVersionDBImpl implements ModelVersionDBService {

	/**
	 * VARCHAR length CONSTANTS
	 */
	private static final int GEN_TYPE_LENGTH = 10;

	/**
	 * JDBC driver used by this implementation = HSQL
	 */
	public static final String HSQL_DRIVERNAME = "org.hsqldb.jdbcDriver";
	
	/**
	 * JDBC driver used by this implementation for MySQL
	 */
	public static final String MYSQL_DRIVERNAME = "com.mysql.jdbc.Driver";
	
	/**
	 * JDBC driver used by this implementation for Oracle
	 */
	public static final String ORACLE_DRIVERNAME = "oracle.jdbc.driver.OracleDriver";

	/*
	 * Constants
	 */
	private static final int FIRST_REVISION = 1;

	private static final boolean DEFAULT_VERSION_SPECIFIC_FLAG = true;
	
	private static final boolean DEFAULT_SRC_VERSION_SPECIFIC_FLAG = true;
	
	private static final boolean DEFAULT_DEST_VERSION_SPECIFIC_FLAG = true;
	
	// OSGi fields
	private BundleContext m_context;
	
	// Logger
	private Logger m_logger;

	// Connection fields
	private ConnectionDef m_connection;
	
	// component status
	
	private boolean m_enable;
	private boolean m_started;

	// Config properties
	
	private boolean m_allowAdmin;
	
	/*
	 * Transaction Management
	 */
	private boolean m_transaction;
	private Map<String, ConnectionDef> /* <URL, ConnectionDef> */ m_connections = new HashMap<String, ConnectionDef>();

	public ModelVersionDBImpl(BundleContext bc) {
		m_context = bc;
		
		m_logger = new Logger(m_context, "Registry logger " + m_context.getBundle().getBundleId(), Logger.DEBUG);
		
		m_enable = true;
	}

	private void loadDriverClass() {
		if (m_connection.getBaseType() == null) {
			throw new IllegalStateException("Database type cannot be discovered.");
		}
		
		String driverName = null;
		if (m_connection.isHSQL()) {
			driverName = HSQL_DRIVERNAME;
		} else if (m_connection.isMySQL()) {
			driverName = MYSQL_DRIVERNAME;
		} else if (m_connection.isOracle()) {
			driverName = ORACLE_DRIVERNAME;
		} else {
			throw new IllegalStateException("Database type " + m_connection.getBaseType() + " is not supported.");
		}
		
		try {
			m_context.getBundle().loadClass(driverName).newInstance(); 

		} catch (ClassNotFoundException c) {
			m_logger.log(Logger.ERROR, " Could not find the database driver cause:\n",
					c);
		} catch (InstantiationException e) {
			m_logger.log(Logger.ERROR, " Could not instantiate the database driver cause:\n",
					e);
		} catch (IllegalAccessException e) {
			m_logger.log(Logger.ERROR, " unexpected exception:\n", e);
		}
	}
	
	public void setConnectionURL(String url) {
		setConnectionURLInternal(url, null, null);
	}
	
	public void setConnectionURL(String url, String login, String password) {
		checkLoginAndPassword(login, password);
		setConnectionURLInternal(url, login, password);
	}

	private synchronized void setConnectionURLInternal(String url, String login,
			String password) {
		if (m_started && isConnected() && !hasTransaction()) {
			closeConnection(m_connection);
		}
		
		setCurrentConnection(new ConnectionDef(url, login, password, m_logger));
		
		configureConnection();
	}

	private void setCurrentConnection(ConnectionDef connectionDef) {
		String url = connectionDef.getURL();
		m_connection = m_connections.get(url);
		if (m_connection != null)
			return;
		
		m_connection = connectionDef;
		m_connections.put(url, m_connection);
	}

	public void setConnectionURL(String dbType, String host, int port,
			String dbName) throws TransactionException {
		setConnectionURLInternal(dbType, host, port, dbName, null, null);
	}

	public void setConnectionURL(String dbType, String host, int port,
			String dbName, String login, String password) throws TransactionException {
		checkLoginAndPassword(login, password);
		setConnectionURLInternal(dbType, host, port, dbName, login, password);
	}

	private void checkLoginAndPassword(String login, String password) {
		boolean error = false;
		RuntimeException except = null;
		if (login == null) {
			error = true;
			except = new IllegalArgumentException("Login must not be null.");
		}
		if (password == null) {
			error = true;
			except = new IllegalArgumentException("Password must not be null.");
		}
		
		if (error && m_started && isConnected()) {
			if (hasTransaction())
				m_connection = null;
			else
				closeConnection(m_connection);
		}
		if (except != null)
			throw except;
	}

	private synchronized void setConnectionURLInternal(String dbType, String host,
			int port, String dbName, String login, String password) throws TransactionException {
		if (m_started && isConnected() && !hasTransaction())
			closeConnection(m_connection);
		
		setCurrentConnection(new ConnectionDef(dbType, host, port, dbName, login, password, m_logger));
		
		configureConnection();
	}

	private void configureConnection() {

		try {
			loadDriverClass();
		} catch (IllegalStateException e) {
			throw new IllegalArgumentException("Unsupported database type:" + m_connection.getBaseType(), e);
		}
		
		if (m_started)
			openConnection(m_connection);
	}

	public String getConnectionURL() {
		if (m_connection == null)
			return null;
		
		return m_connection.getURL();
	}
	
	public String getLogin() {
		if (m_connection == null)
			return null;
		
		return m_connection.getLogin();
	}
	
	public synchronized boolean isConnected() {
		return (m_connection != null) && (!m_connection.isClosed());
	}

	public void start() {

		// Reset the connection if closed
		for (ConnectionDef connection : m_connections.values()) {
			openConnection(connection);
		}
		
		m_started = true;
	}

	private void openConnection(ConnectionDef connection) {
		connection.openConnection();
		initConnection(connection);
		try {
			initSavepointPrefix();
		} catch (ModelVersionDBException e) {
			m_logger.log(Logger.ERROR, "Transaction support cannot be initialized for " + m_connection, e);
		}
	}
	
	private void initConnection(ConnectionDef connection) {
		try {
			int clientIdx = genNewIdx(TableConstants.TYPE_CLIENT_NAME);
			connection.setClientIdx(clientIdx);
			
			if (!m_connection.isMySQL()) {
				// ensure that sequence is created only once and there is
				// one per client
				String lastSeqName = LAST_REV_SEQ_NAME + "_" + clientIdx;
				connection.setLastSeqName(lastSeqName);
				try {
					executeCreateSequence(lastSeqName, 1);
				} catch (SQLException sqlE) {
					// sequence may already exists
					m_logger.log(Logger.DEBUG, "Cannot create sequence "
							+ lastSeqName + ": " + sqlE.getMessage(),
							sqlE);
				}

				// reset sequence to start from 1
				executeResetSequence(lastSeqName, 1);
			}
			
			connection.commit();
		} catch (SQLException e) {
			m_logger.log(Logger.ERROR, "Cannot init the connection to database using URL " + 
					connection.getURL() + ": " + e.getMessage(), e);
		} catch (ModelVersionDBException e) {
			m_logger.log(Logger.ERROR, "Cannot generate a new client index : "
					+ e.getMessage(), e);
		}
	}

	private void executeCreateSequence(String seqName, Integer start) throws SQLException {
		StringBuffer querySB = new StringBuffer();
		querySB.append("CREATE SEQUENCE ");
		querySB.append(seqName);
		if (start != null) {
			querySB.append(" START WITH ");
			querySB.append(m_connection.getTypeUtil().getSQLValue(start));
		}
		
		executeUpdate(getEndQuery(querySB, m_connection), m_connection);
	}

	private void executeResetSequence(String seqName, int newStart) throws SQLException {
		if (!m_connection.isOracle()) {
			executeUpdate(getResetSequenceQuery(seqName, newStart), m_connection);
			return;
		}
		
		// Oracle cannot restart a sequence
		executeUpdate(getDropSequenceQuery(seqName, m_connection), m_connection);
		executeCreateSequence(seqName, newStart);
	}

	public void stop() {
		m_started = false;
		
		m_connection = null;
		clearTransaction();
		
		// close opened connection
		for (ConnectionDef connection : m_connections.values()) {
			closeConnection(connection);
		}
		
		m_connections.clear();
	}

	private synchronized void closeConnection(ConnectionDef connection) {
		try {
			if (!connection.isClosed()) {
				rollbackAllInternalTransactions(connection);
				
				// remove sequences
				if (!connection.isMySQL()) {
					executeUpdate(getDropSequenceQuery(connection.getLastSeqName(), connection), connection);
				}
				
				connection.close();
			}
		} catch (Exception e) {
			m_logger.log(Logger.ERROR, "[ERROR IN DATABASE ACCESS] Cannot close the connection : "
							+ e.getMessage(), e);
		}
		
		if (connection == m_connection) {
			m_connection = null;
			m_connections.remove(connection.getURL());
		}
	}

	private String getDropSequenceQuery(String seqName, ConnectionDef connection) {
		StringBuffer querySB = new StringBuffer();
		querySB.append("DROP SEQUENCE ");
		querySB.append(seqName);
		if (connection.isHSQL()) {
			querySB.append(" IF EXISTS"); 
		}
		
		return getEndQuery(querySB, connection);
	}

	private synchronized void clearTransaction() {
		m_transaction = false;
		
		// close all opened connections except current one
		for (ConnectionDef connection : m_connections.values()) {
			if ((m_connection == null) || 
					!m_connection.getConnection().equals(connection.getConnection()))
				closeConnection(connection);
		}
		m_connections.clear();
		
		// add current connection
		if (m_connection != null)
			m_connections.put(m_connection.getURL(), m_connection);
	}

	private ResultSet executeQueryWithScrollable(String sql, boolean printError)
			throws SQLException {
		m_logger.log(Logger.DEBUG, "Execute sql query : " + sql);
		Statement stmt_scrollable = null;
		try {
			stmt_scrollable = m_connection.getConnection().createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			return stmt_scrollable.executeQuery(sql);
		} catch (SQLException e) {
//			m_logger.log(Logger.DEBUG, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeQuery("
//							+ sql + ") : " + e.getMessage(), e);
			if (printError) {
				m_logger.log(Logger.ERROR, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeQuery("
							+ sql + ") : " + e.getMessage(), e);
			}
			
			try {
				if (stmt_scrollable != null)
					stmt_scrollable.close();
			} catch (RuntimeException e1) {
				// ignore it
			}
			
			throw e;
		}
	}
	
	private ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql, true);
	}

	private ResultSet executeQuery(String sql, boolean printError) throws SQLException {
		m_logger.log(Logger.DEBUG, "Execute sql query : " + sql);
		Statement stat = null;
		try {
			stat = m_connection.getConnection().createStatement();
			return stat.executeQuery(sql);
		} catch (SQLException e) {
			m_logger.log(Logger.DEBUG, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeQuery("
							+ sql + ") : " + e.getMessage(), e);
			if (printError)
				m_logger.log(Logger.ERROR, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeQuery("
							+ sql + ") : " + e.getMessage(), e);
			
			try {
				if (stat != null)
					stat.close();
			} catch (RuntimeException e1) {
				// ignore it
			}
			
			throw e;
		} 
	}

	private int executeUpdate(String sql, ConnectionDef connection) throws SQLException {
		m_logger.log(Logger.DEBUG, "Execute sql query : " + sql);
		
		int result = -1;
		Statement stat = null;
		try {
			stat = connection.getConnection().createStatement();
			result = stat.executeUpdate(sql);
		} catch (SQLException e) {
			m_logger.log(Logger.ERROR, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeUpdate("
							+ sql + ") : " + e.getMessage(), e);
			throw e;
		} finally {
			try {
				if (stat != null)
					stat.close();
			} catch (RuntimeException e1) {
				// ignore it
			}
		}

		return result;
	}

	/*
	 * Object Management
	 */

	public synchronized int createObject(UUID objectId, UUID typeId,
			Map<String, Object> stateMap, boolean isType) throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkObjectIdParam(objectId);
		beginInternalTransaction();
		
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			String typeTabName = checkTypeTableExist(typeId);

			checkTableExist(OBJ_TAB);

			if (objExists(objectId)) {
				commitInternalTransaction();
				throw new IllegalArgumentException("Object with id = "
						+ objectId + " already exists.");
			}

			Revision rev = addObjToObjTable(objectId, typeId, isType);

			checkTableExist(ATTR_TAB);
			Map<String, String> attrMap = getAttributes(stateMap);
			Map<String, String> attrColMap = checkAttributes(typeId, attrMap, true);
			Map<String, Boolean> attrVSMap = getAttrVSFlag(typeId, attrMap);

			try {
				storeState(rev, typeTabName, stateMap, attrColMap, attrVSMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}
			commitInternalTransaction();
			
			return rev.getRev();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}


	/**
	 * This method should be called after checking of parameters.
	 */
	private void beginInternalTransaction() {
		if (!m_transaction)
			return;
		
		m_connection.createNewSavepoint();
	}

	private void initSavepointPrefix() throws ModelVersionDBException {
		if (m_connection.getSavepointPrefix() == null) {
			int newIdx = genNewIdx(TableConstants.TYPE_SAVEPOINT_NAME);
			m_connection.setSavepointPrefix(SAVEPOINT_PREFIX + newIdx + "_");
		}
	}

	private int genNewIdx(String idxType) throws ModelVersionDBException {
		try {
			checkTableExist(GEN_TAB);

			// get last used savepoint index
			StringBuffer querySB = getBeginSelectQuery(false, GEN_TAB,
					GEN_LAST_IDX_COL);
			addWherePart(querySB);
			addEqualPart(querySB, GEN_TYPE_COL, idxType);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);
			boolean hasLastSavePopintIdx = rs.next();
			
			if (!hasLastSavePopintIdx) {
				
				// TODO workaround for a bug with mysql databases
				addDefaultEntries(GEN_TAB);
				
				querySB = getBeginSelectQuery(false, GEN_TAB,
						GEN_LAST_IDX_COL);
				addWherePart(querySB);
				addEqualPart(querySB, GEN_TYPE_COL, idxType);
				query = getEndQuery(querySB, m_connection);
				rs = executeQuery(query);
				boolean hasEntry = rs.next();
				if (!hasEntry)
					throw new IllegalStateException("Entry " + idxType
						+ " should exists in table " + GEN_TAB);
			}

			int new_idx = rs.getInt(1) + 1;
			rs.close();

			// store new table name index
			querySB = getBeginUpdateQuery(GEN_TAB,
					new Assignment(GEN_LAST_IDX_COL));
			addWherePart(querySB);
			addEqualPart(querySB, GEN_TYPE_COL, idxType);
			query = getEndQuery(querySB, m_connection);

			PStatement ps = createPreparedStatement(query);
			ps.getStatement().setInt(1, new_idx);
			ps.executeUpdate();
			ps.close();

			return new_idx;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void rollbackToSavepoint(Savepoint savepoint) {
		try {
			m_logger.log(Logger.DEBUG, "Rollback to savepoint " + savepoint.getSavepointName());
			m_connection.getConnection().rollback(savepoint);
			m_connection.getConnection().releaseSavepoint(savepoint);
		} catch (SQLException e) {
			m_logger.log(Logger.ERROR, "Rollback to savepoint failed.", e);
		}
	}
	
	private void commitInternalTransaction() {
		if (!m_transaction) {
			try {
				m_connection.commit();
			} catch (SQLException e) {
				m_logger.log(Logger.ERROR, "Commit failed : ", e);
				rollbackInternalTransaction(e);
			}
			return;
		}
		
		Savepoint savepoint = m_connection.popLastSavepoint();
		try {
			String savepointName = savepoint.getSavepointName();
			m_logger.log(Logger.DEBUG, "Commit savepoint " + savepointName);
			if (!m_connection.isOracle()) {
				try {
					m_connection.getConnection().releaseSavepoint(savepoint);
				} catch (Exception e) {
					m_logger.log(Logger.ERROR, "Release of savepoint " + savepointName + " failed.");
				}
			}
		} catch (SQLException e) {
			m_logger.log(Logger.ERROR, "Commit failed : ", e);
			rollbackToSavepoint(savepoint);
		}
	}

	private void rollbackInternalTransaction(Exception e) {
		if (!m_transaction) {
			try {
				m_connection.rollback();
			} catch (SQLException sqlError) {
				m_logger.log(Logger.ERROR, "Internal rollback failed.", sqlError);
			}
			return;
		}
		
		Savepoint savepoint = m_connection.popLastSavepoint();
		rollbackToSavepoint(savepoint);
	}
	
	private void rollbackAllInternalTransactions(ConnectionDef connection) {
		if (!m_transaction) {
			try {
				connection.rollback();
			} catch (SQLException sqlError) {
				m_logger.log(Logger.ERROR, "Internal rollback failed.", sqlError);
			}
			return;
		}
		
		for (Savepoint savepoint = connection.popLastSavepoint(); savepoint != null; 
			savepoint = connection.popLastSavepoint()) {
			try {
				if (!connection.isOracle())
					connection.getConnection().releaseSavepoint(savepoint);
			} catch (SQLException e) {
				try {
					m_logger.log(Logger.ERROR, "Release savepoint " + savepoint.getSavepointName() + " failed.");
				} catch (SQLException e1) {
					// ignore it
					m_logger.log(Logger.ERROR, "Release savepoint failed.", e);
				}
			}
			connection.resetSavepoints();
		}
	}

	private void storeState(Revision rev, String typeTabName,
			Map<String, Object> stateMap, Map<String, String> attrColMap, 
			Map<String, Boolean> attrVSMap)
			throws SQLException, ModelVersionDBException {
		
		// get attribute values in DB
		StringBuffer querySB = getBeginSelectQuery(false, typeTabName, "*");
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, rev.getId());
		if (rev.getRev() != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, rev.getRev());
		}
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = executeQuery(query);
		boolean rowExist = rs.next();
		rs.close();
		
		// insert or update object row
		if (!rowExist) {
			// add new row
			rev = new Revision(rev.getId(), rev.getTypeId(), rev.getRev());
			
			query = getInsertQuery(typeTabName, 
					new Assignment(PERTYPE_OBJ_ID_COL, rev.getId(), m_connection.getTypeUtil()),
					new Assignment(PERTYPE_REV_COL, rev.getRev(), m_connection.getTypeUtil()));
			executeUpdate(query, m_connection);
		}
		
		if ((stateMap == null) || (stateMap.isEmpty())) {
			// work is done if there is no attributes
			return;
		}
		
		List<Assignment> assigns = new ArrayList<Assignment>();
		List<Object> values = new ArrayList<Object>();
		for (String attr : stateMap.keySet()) {
			String colName = attrColMap.get(attr);
			assigns.add(new Assignment(colName));
			values.add(stateMap.get(attr));
		}
		updateAttrValuesInternal(rev, typeTabName, assigns, values);
		
		// manage case of not version specific attributes
		if (rev.getRev() == ModelVersionDBService.ALL)
			return; // work already done for not version specific attributes
		
		assigns = new ArrayList<Assignment>();
		values = new ArrayList<Object>();
		for (String attr : stateMap.keySet()) {
			String colName = attrColMap.get(attr);
			Boolean isVersionSpec = attrVSMap.get(attr);
			if ((isVersionSpec != null) && (isVersionSpec))
				continue;
				
			assigns.add(new Assignment(colName));
			values.add(stateMap.get(attr));
		}
		if (!assigns.isEmpty())
			updateAttrValuesInternal(rev.getRev(ModelVersionDBService.ALL), typeTabName, assigns, values);
	}

	private void updateAttrValuesInternal(Revision rev, String typeTabName,
			List<Assignment> assigns, List<Object> values) throws SQLException,
			ModelVersionDBException {
		StringBuffer querySB;
		String query;
		querySB = getBeginUpdateQuery(typeTabName, assigns
					.toArray(new Assignment[assigns.size()]));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, rev.getId());
		if (rev.getRev() != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, rev.getRev());
		}
		query = getEndQuery(querySB, m_connection);
		PStatement prepare = createPreparedStatement(query);
		for (int i = 0; i < values.size(); i++) {
			Object value = values.get(i);
			int idx = i + 1;
			setValue(prepare, idx, value);
		}
		try {
			prepare.executeUpdate();
		} catch (IOException e) {
			throw new ModelVersionDBException(e);
		}
		prepare.close();
	}

	private PStatement createPreparedStatement(String query)
			throws SQLException {
		PreparedStatement prepare = m_connection.getConnection().prepareStatement(query);

		return new PStatement(prepare);
	}

	private void setValue(PStatement prepare, int idx, Object value)
			throws SQLException, ModelVersionDBException {
		if (value == null) {
			setSerializableObject(prepare, idx, value);
			return;
		}
		
		if (value instanceof Boolean) {
			if (m_connection.isOracle()) {
				String strVal = ((Boolean) value) ? "T" : "F";
				
				prepare.getStatement().setString(idx, strVal);
				return;
			}
				
			prepare.getStatement().setBoolean(idx, (Boolean) value);
			return;
		}
		
		if (value instanceof UUID) {
			prepare.getStatement().setString(idx, ((UUID) value).toString());
			return;
		}

		if (value instanceof String) {
			prepare.getStatement().setString(idx, (String) value);
			return;
		}

		if (value instanceof Integer) {
			prepare.getStatement().setInt(idx, (Integer) value);
			return;
		}
		
		if (value instanceof java.util.Date) {
			prepare.getStatement().setLong(idx, ((java.util.Date) value).getTime());
			return;
		}

		if (value instanceof Long) {
			prepare.getStatement().setLong(idx, (Long) value);
			return;
		}

		if (value instanceof java.sql.Time) {
			prepare.getStatement().setTime(idx, (java.sql.Time) value);
			return;
		}

		if (value instanceof java.sql.Date) {
			prepare.getStatement().setDate(idx, (java.sql.Date) value);
			return;
		}

		if (value instanceof Serializable) {
			setSerializableObject(prepare, idx, value);
			return;
		}

		prepare.getStatement().setObject(idx, value);
	}

	private void setSerializableObject(PStatement prepare, int idx, Object obj)
			throws ModelVersionDBException, SQLException {
		if (obj == null) {
			prepare.getStatement().setNull(idx, m_connection.getTypeUtil().getSQLtypeIntForObject());
			return;
		}
		
		if (!(obj instanceof Serializable)) {
			throw new IllegalArgumentException(obj + " is not serializable object.");
		}
		
		if (ModelVersionDBService.MYSQL_TYPE.equals(m_connection.getTypeUtil()))
			prepare.getStatement().setObject(idx, obj);
		else
			prepare.getStatement().setBytes(idx, TypeUtil.getSerValue((Serializable) obj));
	}

	private Map<String, String> checkAttributes(UUID typeId,
			Map<String, String> attrMap, boolean updateDefinitions) throws SQLException,
			ModelVersionDBException {
		Map<String, String> attrColMap = new HashMap<String, String>(); /*
		 * <attribute
		 * name,
		 * attribute
		 * column
		 * name>
		 */
		if (attrMap.isEmpty()) {
			createTypeTable(getTypeTabName(typeId), null);
			return attrColMap;
		}
		
		// get attributes in DB
		StringBuffer querySB = getBeginSelectQuery(false, ATTR_TAB,
				ATTR_ATTR_NAME_COL, ATTR_ATTR_TYPE_COL, ATTR_ATTR_TAB_NAME_COL);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = executeQuery(query);

		Map<String, String> savedAttrMap = new HashMap<String, String>(); /*
																			 * <attribute
																			 * name,
																			 * attribute
																			 * column
																			 * name>
																			 */
		Map<String, String> attrTotalMap = new HashMap<String, String>(); /*
																			 * <attribute
																			 * column
																			 * name,
																			 * attribute
																			 * type>
																			 */
		Map<String, String> attrToMigrateMap = new HashMap<String, String>(); /*
		 * <attribute
		 * name,
		 * new attribute
		 * type>
		 */
		int lastIdx = -1;
		while (rs.next()) {
			String attr = rs.getString(ATTR_ATTR_NAME_COL);
			String attrType = rs.getString(ATTR_ATTR_TYPE_COL);
			String colName = rs.getString(ATTR_ATTR_TAB_NAME_COL);
			Integer idx = getAttrIdx(colName);
			attrTotalMap.put(colName, attrType);
			if (idx > lastIdx)
				lastIdx = idx;
			if (attrMap.containsKey(attr)) {
				String newType = attrMap.get(attr);
				if (m_connection.getTypeUtil().incompatibleTypes(attrType, newType)) {
					if (m_connection.getTypeUtil().migratableToNewType(attrType, newType)) {
						attrToMigrateMap.put(attr, newType);
					} else
						throw new ModelVersionDBException("Found persist type "
							+ attrType + " and new type " + newType
							+ " for attribute " + attr + " of type " + typeId);
				}
				savedAttrMap.put(attr, colName);
				attrColMap.put(attr, colName);
			}
		}
		rs.close();
		
		if (!updateDefinitions)
			return attrColMap;
		
		// change attribute definition
		if (!attrToMigrateMap.isEmpty()) {
			for (String attr : attrToMigrateMap.keySet()) {
				lastIdx++;
				String newType = attrToMigrateMap.get(attr);
				String colName = getAttrColName(lastIdx);
				String prevColName = attrColMap.get(attr);
				
				// update mapping between column names and attribute names
				attrColMap.put(attr, colName);
				attrTotalMap.put(colName, newType);
				
				migrateCol(typeId, newType, colName, prevColName);
			}
		}

		// get list of not already stored attributes
		List<String> addedAttrs = new ArrayList<String>();
		for (String attr : attrMap.keySet()) {
			if (!savedAttrMap.containsKey(attr)) {
				addedAttrs.add(attr);
			}
		}

		// store new attributes definition
		query = getInsertQuery(ATTR_TAB, 
				new Assignment(ATTR_TYPE_ID_COL, typeId, m_connection.getTypeUtil()), 
				new Assignment(ATTR_ATTR_NAME_COL),
				new Assignment(ATTR_ATTR_TYPE_COL), 
				new Assignment(ATTR_ATTR_TAB_NAME_COL),
				new Assignment(ATTR_VERSION_SPEC_COL, DEFAULT_VERSION_SPECIFIC_FLAG, m_connection.getTypeUtil()));
		PreparedStatement prepare = m_connection.getConnection().prepareStatement(query);
		int newIdx = lastIdx;
		for (String attr : addedAttrs) {
			newIdx++;
			String attrType = attrMap.get(attr);
			String colName = getAttrColName(newIdx);
			attrTotalMap.put(colName, attrType);
			attrColMap.put(attr, colName);

			prepare.setString(1, attr);
			prepare.setString(2, attrType);
			prepare.setString(3, colName);
			prepare.executeUpdate();
		}
		prepare.close();

		if (tableExist(getTypeTabName(typeId))) {
			// Add missing attribute columns
			for (String attr : addedAttrs) {
				String attrType = attrMap.get(attr);
				String colName = attrColMap.get(attr);
				
				query = getAddColumnQuery(getTypeTabName(typeId), colName, attrType);
				executeUpdate(query, m_connection);
			}
		} else {
			// create table for objects of type typeId
			createTypeTable(getTypeTabName(typeId), attrTotalMap);
		}

		return attrColMap;
	}

	private int getAttrIdx(String colName) {
		return Integer.parseInt(colName.substring(TableConstants.ATTR_COL_PREFIX
				.length()));
	}

	private void migrateCol(UUID typeId, String newType, String colName,
			String prevColName) throws ModelVersionDBException, SQLException {
		String query;
		// Add a new column for attribute values
		query = getAddColumnQuery(getTypeTabName(typeId), colName, newType);
		executeUpdate(query, m_connection);
		
		// Update associations between attribute names and columns
		StringBuffer querySB = getBeginUpdateQuery(ATTR_TAB, 
				new Assignment(ATTR_ATTR_TAB_NAME_COL), new Assignment(ATTR_ATTR_TYPE_COL));
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, ATTR_ATTR_TAB_NAME_COL, prevColName);
		query = getEndQuery(querySB, m_connection);
		PStatement ps = createPreparedStatement(query);
		ps.getStatement().setString(1, colName);
		ps.getStatement().setString(2, newType);
		try {
			ps.executeUpdate();
		} catch (IOException e) {
			throw new ModelVersionDBException(e);
		}
		ps.close();
		
		// Copy previous values into the new column
		query = getCopyColumnQuery(getTypeTabName(typeId), prevColName, colName);
		executeUpdate(query, m_connection);
		
		// Remove unused column which represented attribute values
		query = getRemoveColumnQuery(getTypeTabName(typeId), prevColName);
		executeUpdate(query, m_connection);
	}

	private String getAttrColName(int lastIdx) {
		return TableConstants.ATTR_COL_PREFIX + lastIdx;
	}
	
	private void close(ResultSet rs) {
		if (rs != null)
			try {
				close(rs.getStatement());
			} catch (SQLException e) {
				// ignore it
			}
	}

	private void close(PStatement rs) {
		if (rs != null)
			try {
                            rs.close();
			} catch (SQLException e) {
				// ignore it
			}
	}

	private void close(Statement rs) {
		if (rs != null)
			try {
				rs.close();
			} catch (SQLException e) {
				// ignore it
			}
	}
	
	private String getCopyColumnQuery(String typeTabName, String prevColName,
			String colName) {
		StringBuffer sb = new StringBuffer("UPDATE ");
		sb.append(typeTabName);
		sb.append(" SET ");
		sb.append(colName);
		sb.append(" = ");
		sb.append(prevColName);
		
		return getEndQuery(sb, m_connection);
	}

	private String getAddColumnQuery(String tableName, String colName,
			String attrType) {
		return getAlterTableColumnQuery(tableName, colName, attrType, true);
	}
	
	private String getRemoveColumnQuery(String tableName, String colName) {
		return getAlterTableColumnQuery(tableName, colName, null, false);
	}

	private String getAlterTableColumnQuery(String tableName, String colName,
			String attrType, boolean add) {
		// Remove type annotations
	    String attributeType = TypeUtil.removeTypeAnnotations(attrType);
		
		// construct query
		StringBuffer sb = new StringBuffer("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" ");
		if (add)
			sb.append("ADD");
		else
			sb.append("DROP");
		sb.append(" COLUMN ");
		sb.append(colName);
		if (add) {
			sb.append(" ");
			sb.append(attributeType);
		}
		
		return getEndQuery(sb, m_connection);
	}

	private void createTypeTable(String typeTabName,
			Map<String, String> attrTotalMap) throws SQLException {
		if (tableExist(typeTabName))
			return;
		
		StringBuffer querySB = getBeginCreateTableQuery(typeTabName);
		addCreateTableColumnPart(querySB, PERTYPE_OBJ_ID_COL, TypeUtil
				.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, PERTYPE_REV_COL, SQLTypes.INT_SQL_TYPE_DEF, false);
		if ((attrTotalMap != null) && (!attrTotalMap.isEmpty())) {
			addColDefSeparator(querySB);
			for (Iterator<String> it = attrTotalMap.keySet().iterator(); it.hasNext(); ) {
				String colName = it.next();
				String colType = TypeUtil.removeTypeAnnotations(attrTotalMap.get(colName));
				addCreateTableColumnPart(querySB, colName, colType, false);
				if (it.hasNext())
					addColDefSeparator(querySB);
			}
		}
		addMultiplePKPart(querySB, "PK_" + typeTabName, PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL);
		String query = getEndCreateTableQuery(querySB);

		executeUpdate(query, m_connection);
	}

	private Map<String, String> getAttributes(Map<String, Object> stateMap)
			throws ModelVersionDBException {
		Map<String, String> attrMap = new HashMap<String, String>();
		if ((stateMap == null) || stateMap.isEmpty())
			return attrMap;
		
		for (String attr : stateMap.keySet()) {
			Object value = stateMap.get(attr);
			try {
				String attrType = m_connection.getTypeUtil().getSQLType(value);
				attrMap.put(attr, attrType);
			} catch (ModelVersionDBException e) {
				throw new ModelVersionDBException("Attribute " + attr
						+ " has no serializable type.", e);
			}
		}
		return attrMap;
	}

	/**
	 * Return name of table where objects of specified should be stored.
	 * This method doesn't create this table if it doesn't already exist.
	 * 
	 * @param typeId
	 * @return
	 * @throws ModelVersionDBException
	 */
	private String checkTypeTableExist(UUID typeId) throws ModelVersionDBException {
		if (typeExistsInTypeTable(typeId))
			return getTypeTabName(typeId);

		String typeTabName = genTypeTabName();
		try {
			checkTableExist(TYPE_TAB);
			String query = getInsertQuery(TYPE_TAB, 
					new Assignment(TYPE_TYPE_ID_COL, typeId, m_connection.getTypeUtil()), 
					new Assignment(TYPE_TAB_NAME_COL, typeTabName, m_connection.getTypeUtil()), 
					new Assignment(TYPE_LINK_SRC_VERSION_SPEC_COL, DEFAULT_SRC_VERSION_SPECIFIC_FLAG, m_connection.getTypeUtil()), 
					new Assignment(TYPE_LINK_DEST_VERSION_SPEC_COL, DEFAULT_DEST_VERSION_SPECIFIC_FLAG, m_connection.getTypeUtil()));
			
			executeUpdate(query, m_connection);
			return typeTabName;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private String getTypeTabName(UUID typeId) throws ModelVersionDBException {
		StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB, TYPE_TAB_NAME_COL);
		addWherePart(querySB);
		addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);

		try {
			ResultSet rs = executeQuery(query);

			if (!rs.next())
				throw new IllegalArgumentException("Type with id = " + typeId
						+ "doesn't exist in database.");

			String result = rs.getString(1);
			rs.close();
			
			return result;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private String genTypeTabName() throws ModelVersionDBException {
		return "T_" + genNewIdx(TableConstants.TYPE_TYPE_NAME);
	}

	private StringBuffer getBeginUpdateQuery(String tableName,
			Assignment... assigns) {
		
		setTypeUtilOnAssigns(assigns);
		
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE ");
		sb.append(tableName);
		sb.append(" SET ");
		for (int i = 0; i < assigns.length; i++) {
			sb.append(assigns[i].getColumn());
			sb.append(" = ");
			sb.append(assigns[i].getAssignStrValue());

			if (i != assigns.length - 1) {
				sb.append(", ");
			}
		}
		
		return sb;
	}

	private void setTypeUtilOnAssigns(Assignment... assigns) {
		TypeUtil typeUtil = m_connection.getTypeUtil();
		for (Assignment assign : assigns) {
			assign.setTypeUtil(typeUtil);
		}
	}

	private boolean typeExistsInTypeTable(UUID typeId)
			throws ModelVersionDBException {
		try {
			StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB,
					TYPE_TYPE_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);
			boolean typeExist = rs.next();
			rs.close();
			
			return (typeExist);

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void checkTableExist(String tableName) throws SQLException {
		if (!tableExist(tableName)) {

			StringBuffer querySB = getBeginCreateTableQuery(tableName);
			addColumnsDef(querySB, tableName);
			String query = getEndCreateTableQuery(querySB);

			executeUpdate(query, m_connection);

			addDefaultEntries(tableName);
		}
	}

	private void addDefaultEntries(String tableName) throws SQLException {
		if (GEN_TAB.equals(tableName)) {
			String query = getInsertQuery(tableName, 
					new Assignment(	GEN_TYPE_COL, TableConstants.TYPE_TYPE_NAME, m_connection.getTypeUtil()), 
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query, m_connection);
			
			query = getInsertQuery(tableName, new Assignment(
					GEN_TYPE_COL, TableConstants.TYPE_SAVEPOINT_NAME, m_connection.getTypeUtil()), 
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query, m_connection);
			
			query = getInsertQuery(tableName, new Assignment(
					GEN_TYPE_COL, TableConstants.TYPE_CLIENT_NAME, m_connection.getTypeUtil()), 
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query, m_connection);
		}
	}

	private void addColumnsDef(StringBuffer querySB, String tableName) {
		if (OBJ_TAB.equals(tableName)) {
			addCreateTableColumnPart(querySB, OBJ_OBJ_ID_COL, TypeUtil
					.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), true);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, OBJ_TYPE_ID_COL, TypeUtil
					.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, OBJ_IS_TYPE_COL, 
					m_connection.getTypeUtil().getBooleanDef(), false);
			return;
		}

		if (TYPE_TAB.equals(tableName)) {
			addCreateTableColumnPart(querySB, TYPE_TYPE_ID_COL, TypeUtil
					.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), true);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, TYPE_TAB_NAME_COL, TypeUtil
					.getVarcharDef(TypeUtil.TABLE_NAME_LENGTH), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, TYPE_LINK_SRC_VERSION_SPEC_COL, 
					m_connection.getTypeUtil().getBooleanDef(), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, TYPE_LINK_DEST_VERSION_SPEC_COL, 
					m_connection.getTypeUtil().getBooleanDef(), false);
			return;
		}

		if (GEN_TAB.equals(tableName)) {
			addCreateTableColumnPart(querySB, GEN_TYPE_COL, TypeUtil
					.getVarcharDef(GEN_TYPE_LENGTH), true);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, GEN_LAST_IDX_COL,
					SQLTypes.INT_SQL_TYPE_DEF, false);
			return;
		}

		if (ATTR_TAB.equals(tableName)) {
			addCreateTableColumnPart(querySB, ATTR_TYPE_ID_COL, TypeUtil
					.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, ATTR_ATTR_NAME_COL, TypeUtil
					.getVarcharDef(TypeUtil.STRING_VARCHAR_LENGTH), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, ATTR_ATTR_TAB_NAME_COL, TypeUtil
					.getVarcharDef(TypeUtil.TABLE_NAME_LENGTH), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, ATTR_ATTR_TYPE_COL, TypeUtil
					.getVarcharDef(35), false);
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, ATTR_VERSION_SPEC_COL, 
					m_connection.getTypeUtil().getBooleanDef(), false);
			addMultiplePKPart(querySB, null, ATTR_TYPE_ID_COL, ATTR_ATTR_NAME_COL);
			return;
		}

		if (LINK_TAB.equals(tableName)) {
			addLinkTableColDef(querySB);
			addMultiplePKPart(querySB, null, LINK_LINK_ID_COL, LINK_REV_COL);
			return;
		}
	}

	private void addLinkTableColDef(StringBuffer querySB) {
		addCreateTableColumnPart(querySB, LINK_LINK_ID_COL, TypeUtil
				.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_TYPE_ID_COL, TypeUtil
				.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_SRC_ID_COL, TypeUtil
				.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_SRC_REV_COL, 
				SQLTypes.INT_SQL_TYPE_DEF, false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_DEST_ID_COL, TypeUtil
				.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_DEST_REV_COL, 
				SQLTypes.INT_SQL_TYPE_DEF, false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_ORDER_COL, 
				SQLTypes.INT_SQL_TYPE_DEF, false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_REV_COL, 
				SQLTypes.INT_SQL_TYPE_DEF, false);
	}

	private static void addMultiplePKPart(StringBuffer querySB, String pkName, String... columns) {
		querySB.append(", CONSTRAINT ");
		if (pkName == null) {
			for (int i = 0; i < columns.length; i++) {
				querySB.append(columns[i].substring(0,
						(columns[i].length() > 5) ? 5 : columns[i].length()));
				if (i != columns.length - 1)
					querySB.append("_");
			}
		} else {
			querySB.append(pkName);
		}
		querySB.append(" PRIMARY KEY(");
		for (int i = 0; i < columns.length; i++) {
			querySB.append(columns[i]);
			if (i != columns.length - 1)
				querySB.append(",");
		}
		querySB.append(")");
	}

	private Revision addObjToObjTable(UUID objectId, UUID typeId, boolean isType)
			throws SQLException {

		String query = getInsertQuery(OBJ_TAB, 
				new Assignment(OBJ_OBJ_ID_COL, objectId, m_connection.getTypeUtil()), 
				new Assignment(OBJ_TYPE_ID_COL, typeId, m_connection.getTypeUtil()),
				new Assignment(OBJ_IS_TYPE_COL, isType, m_connection.getTypeUtil()));

		executeUpdate(query, m_connection);
		
		return new Revision(objectId, typeId, FIRST_REVISION);
	}

	private String getInsertQuery(String tableName, Assignment... assigns) {

		setTypeUtilOnAssigns(assigns);
		
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append(" (");
		StringBuffer valuesSB = new StringBuffer();
		for (int i = 0; i < assigns.length; i++) {
			sb.append(assigns[i].getColumn());
			String value = assigns[i].getAssignStrValue();

			valuesSB.append(value);

			if (i != assigns.length - 1) {
				sb.append(", ");
				valuesSB.append(", ");
			}
		}
		sb.append(") values (");
		sb.append(valuesSB.toString());
		sb.append(")");

		return getEndQuery(sb, m_connection);
	}
	
	private static StringBuffer getInsertQuery(String tableName, String... colToSets) {

		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append(" (");
		for (int i = 0; i < colToSets.length; i++) {
			sb.append(colToSets[i]);
			
			if (i != colToSets.length - 1) {
				sb.append(", ");
			}
		}
		sb.append(")");

		return sb;
	}
	
	private static StringBuffer getBeginInsertQuery(String tableName) {

		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append(" (");

		return sb;
	}
	
	private void addQuerySeparator(StringBuffer querySB) {
		if (!m_connection.isOracle())
			querySB.append("; ");
	}

	private static void addColDefSeparator(StringBuffer querySB) {
		querySB.append(", ");
	}
	
	private static void addCreateTableColumnPart(StringBuffer querySB, String colName,
			String type, boolean primaryKey) {
		addCreateTableColumnPart(querySB,colName, type, primaryKey, false);
	}

	private static void addCreateTableColumnPart(StringBuffer querySB, String colName,
			String type, boolean primaryKey, boolean autoIncrement) {
		querySB.append(colName);
		querySB.append(" ");
		querySB.append(type);
		if (autoIncrement)
			querySB.append(" AUTO_INCREMENT ");
		if (primaryKey)
			querySB.append(" PRIMARY KEY ");
	}
	
	private static StringBuffer getBeginCreateTableQuery(String tableName) {
		return getBeginCreateTableQuery(false, tableName);
	}
	
	private static StringBuffer getBeginCreateTableQuery(boolean inMemory, 
			String tableName) {
		return getBeginCreateTableQuery(inMemory, false, tableName);
	}

	private static StringBuffer getBeginCreateTableQuery(boolean inMemory, 
			boolean temporary, String tableName) {
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE ");
		if (temporary)
			sb.append("TEMPORARY ");
		if (inMemory)
			sb.append("MEMORY ");
		sb.append("TABLE ");
		sb.append(tableName);
		sb.append(" (");

		return sb;
	}

	private static StringBuffer getBeginSelectQuery(boolean distinct, String tableName,
			String... columns) {
		StringBuffer sb = new StringBuffer("SELECT ");
		if (distinct)
			sb.append("DISTINCT ");
		for (int i = 0; i < columns.length; i++) {
			sb.append(columns[i]);
			if (i != columns.length - 1)
				sb.append(", ");
		}
		if (columns.length == 0)
			sb.append("*");
		sb.append(" FROM ");
		if (tableName != null) {
			sb.append(tableName);
			sb.append(" ");
		}

		return sb;
	}

	private String getEndCreateTableQuery(StringBuffer querySB) {
		querySB.append(")");

		return getEndQuery(querySB, m_connection);
	}

	private boolean tableExist(String table) throws SQLException {
		String query = "SELECT count(*) FROM " + table;
		ResultSet rs = null;
		try {
			rs = executeQueryWithScrollable(query, false);
			return true; // the table who has the name = table exists
		} catch (SQLException e) {
			if (e.getErrorCode() == m_connection.getTableNotExistErrorCode())
				return false; // the table who has the name = table doesn't exists
			
			throw e;
		} finally {
			close(rs);
		}
	}

	public synchronized void deleteObject(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		beginInternalTransaction();
		
		try {
			checkTableExist(OBJ_TAB);

			if (!objExists(objectId)) {
				commitInternalTransaction();
				throw new IllegalArgumentException("Object with id = "
						+ objectId + " doesn't exists.");
			}

			UUID typeId = getObjectType(objectId);
			
			// Remove links with source or destination equal to objId
			for (UUID linkTypeId : getIncomingLinkTypes(objectId, ANY)) {
				deleteIncomingLinks(linkTypeId, objectId, ALL);
			}
			for (UUID linkTypeId : getOutgoingLinkTypes(objectId, ANY)) {
				deleteOutgoingLinks(linkTypeId, objectId, ALL);
			}

			// delete object state
			String typeTabName = checkTypeTableExist(typeId);
			StringBuffer querySB = getBeginDeleteQuery(typeTabName);
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);

			// delete object id in objects table
			querySB = getBeginDeleteQuery(OBJ_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private Set<UUID> getOutgoingLinkTypes(UUID objectId, int objRev) throws ModelVersionDBException {
		return getLinkTypesFromObjInternal(objectId, objRev, true);
	}

	private Set<UUID> getLinkTypesFromObjInternal(UUID objectId, int objRev, 
			boolean getOutgoings) throws ModelVersionDBException {
		Set<UUID> resultSet = new HashSet<UUID>();
		
		String objIdCol = LINK_DEST_ID_COL;
		String objRevCol = LINK_DEST_REV_COL;
		if (getOutgoings) {
			objIdCol = LINK_SRC_ID_COL;
			objRevCol = LINK_SRC_REV_COL;
		}
		
		try {
			if (!tableExist(LINK_TAB))
				return resultSet;
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, LINK_TYPE_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, objIdCol, objectId);
			if (objRev != ANY) {
				addAndPart(querySB);
				addEqualPart(querySB, objRevCol, objRev);
			}
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			if (rs.next()) {
				resultSet.add(UUID.fromString(rs.getString(1)));
			}
			rs.close();
			
			return resultSet;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}
	
	private Set<UUID> getIncomingLinkTypes(UUID objectId, int objRev) throws ModelVersionDBException {
		return getLinkTypesFromObjInternal(objectId, objRev, false);
	}

	private int getObjNumber(UUID typeId) throws ModelVersionDBException {
		try {
			StringBuffer querySB = getBeginSelectQuery(false, OBJ_TAB, "count(*)");
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_TYPE_ID_COL, typeId);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			if (!rs.next())
				return 0;
				
			int objNb = rs.getInt(1);
			rs.close();
			
			return objNb;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void deleteType(UUID typeId, String typeTabName, boolean isLinkType) throws ModelVersionDBException,
			SQLException {
		// delete table where stored object states of specified type
		deleteTable(typeTabName);

		// delete attributes definition
		checkTableExist(ATTR_TAB);
		StringBuffer querySB = getBeginDeleteQuery(ATTR_TAB);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);
		executeUpdate(query, m_connection);

		if (isLinkType) {
			// delete links of the specified type
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
		} else {
			// delete objects of the specified type
			querySB = getBeginDeleteQuery(OBJ_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_TYPE_ID_COL, typeId);
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
		}

		// delete the table association of the specified type
		querySB = getBeginDeleteQuery(TYPE_TAB);
		addWherePart(querySB);
		addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
		query = getEndQuery(querySB, m_connection);
		executeUpdate(query, m_connection);
	}

	private final static StringBuffer getBeginDeleteQuery(String tableName) {
		StringBuffer sb = new StringBuffer("DELETE FROM ");
		sb.append(tableName);
		sb.append(" ");

		return sb;
	}

	public UUID getObjectType(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		
		try {
			if (!tableExist(OBJ_TAB))
				throw new IllegalArgumentException(
						"Object with id = " + objectId + " does not exist.");
			
			StringBuffer querySB = getBeginSelectQuery(false, OBJ_TAB, OBJ_TYPE_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			if (!rs.next())
				throw new IllegalArgumentException(
						"Object with id = " + objectId + " does not exist.");

			UUID objTypeId = UUID.fromString(rs.getString(1));
			rs.close();

			return objTypeId;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void deleteTable(String tableName) throws SQLException {
		try {
			executeUpdate(getDropTableQuery(tableName), m_connection);
		} catch (SQLException e) {
			if (m_connection.isOracle() && (e.getErrorCode() == m_connection.getTableNotExistErrorCode()))
				return;
			
			throw e;
		}
	}

	private String getDropTableQuery(String tableName) {
		StringBuffer querySB = new StringBuffer();
		querySB.append("DROP TABLE ");
		if (!m_connection.isOracle())
				querySB.append(" IF EXISTS ");
		querySB.append(tableName);
		
		return getEndQuery(querySB, m_connection);
	}

	public boolean objExists(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, OBJ_TAB, OBJ_OBJ_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query, false);

			return (rs.next());

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return false;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}
	
	public boolean objExists(UUID objectId, int rev)
			throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		checkRevisionParam(rev, false, true);
		
		if ((rev == ModelVersionDBService.ALL) || (rev == ModelVersionDBService.ANY))
			return objExists(objectId);
		
		ResultSet rs = null;
		try {
			if ((rev == ModelVersionDBService.LAST) && !objExists(objectId))
				return false;
			
			// manage LAST revision case
			rev = manageRevNb(objectId, rev, false);
			
			UUID typeId;
			try {
				typeId = getObjectType(objectId);
			} catch (IllegalArgumentException e) {
				// object does not exist
				return false;
			}
			String tableName = checkTypeTableExist(typeId);
			
			StringBuffer querySB = getBeginSelectQuery(false, tableName, PERTYPE_OBJ_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, rev);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query, false);

			return (rs.next());

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return false;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	private void checkRevisionParam(int rev, boolean allowAll, boolean allowAny) {
		if (allowAll && (rev == ModelVersionDBService.ALL))
			return;
		if (!allowAll && (rev == ModelVersionDBService.ALL))
			throw new IllegalArgumentException("Revision ALL is not allowed for this method.");
		
		
		if (allowAny && (rev == ModelVersionDBService.ANY))
			return;
		if (!allowAny && (rev == ModelVersionDBService.ANY))
			throw new IllegalArgumentException("Revision ANY is not allowed for this method.");
		
		if ((rev < 0) && (rev != ModelVersionDBService.LAST))
			throw new IllegalArgumentException("Revision must be greater or equal to 0.");
	}
	
	private boolean isTableNotExistError(SQLException e) {
		return (e.getErrorCode() == m_connection.getTableNotExistErrorCode());
	}

	private void addEqualPart(StringBuffer querySB, String colName, Object value) {
		querySB.append(colName);
		querySB.append(" = ");
		querySB.append(m_connection.getTypeUtil().getSQLValue(value));
	}
	
	private void addEqualPart(StringBuffer querySB, String colName) {
		querySB.append(colName);
		querySB.append(" = ? ");
	}

	private String getEndQuery(StringBuffer querySB, ConnectionDef connection) {
		if (!connection.isOracle())
			querySB.append(";");

		return querySB.toString();
	}

	private static void addWherePart(StringBuffer querySB) {
		querySB.append(" WHERE ");
	}

	private static void addAndPart(StringBuffer querySB) {
		querySB.append(SQLConstants.AND_PART);
	}

	public Set<UUID> getObjects() throws ModelVersionDBException {
		checkConnection();
		
		ResultSet rs = null;
		try {
			if (!tableExist(OBJ_TAB))
				return new HashSet<UUID>();
			
			StringBuffer querySB = getBeginSelectQuery(false, OBJ_TAB, OBJ_OBJ_ID_COL);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			Set<UUID> results = new HashSet<UUID>();
			while (rs.next()) {
				results.add(UUID.fromString(rs.getString(1)));
			}
			return results;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public Set<UUID> getObjects(UUID typeId) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		
		ResultSet rs = null;
		try {
			if (!tableExist(OBJ_TAB))
				return new HashSet<UUID>();
			
			StringBuffer querySB = getBeginSelectQuery(false, OBJ_TAB, OBJ_OBJ_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_TYPE_ID_COL, typeId);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			Set<UUID> results = new HashSet<UUID>();
			while (rs.next()) {
				results.add(UUID.fromString(rs.getString(1)));
			}
			return results;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public Revision addLink(final UUID typeId, final UUID srcId, int srcRev, final UUID destId,
			int destRev, Map<String, Object> stateMap)
			throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkDestIdParam(destId);
		checkRevisionParam(srcRev, true, false); 
		checkRevisionParam(destRev, true, false);
		
		
		// manage LAST revision case
		srcRev = manageRevNb(srcId, srcRev, false);
		destRev = manageRevNb(destId, destRev, false);
		
		// check that source and destination revisions exist
		if (((srcRev == ModelVersionDBService.ALL) && (!objExists(srcId))) ||
			((srcRev != ModelVersionDBService.ALL) && (!objExists(srcId, srcRev))))
			throw new IllegalArgumentException("Source with id = " + srcId + " does not exist.");
		if (((destRev == ModelVersionDBService.ALL) && (!objExists(destId))) ||
			((destRev != ModelVersionDBService.ALL) && (!objExists(destId, destRev))))
				throw new IllegalArgumentException("Destination with id = " + destId + " does not exist.");
		
		beginInternalTransaction();
		
		// manage special case of MySQL
		boolean isMySQL = m_connection.isMySQL();
		
		ResultSet rs = null;
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			String typeTabName = checkTypeTableExist(typeId);
			if (!tableExist(typeTabName))
				createTypeTable(typeTabName, null);
			
			// manage special case of common (shared or non version specific) link types
			StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB, TYPE_LINK_SRC_VERSION_SPEC_COL);
			addWherePart(querySB);
			addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
			String query = getEndQuery(querySB, m_connection);
			rs = executeQuery(query);
			boolean isVersionSpec = DEFAULT_VERSION_SPECIFIC_FLAG;
			if (rs.next()) {
				isVersionSpec = getBoolean(rs, TYPE_LINK_SRC_VERSION_SPEC_COL);
			}
			rs.close();
			
			// same attribute value (links) for all source revisions = not version specific (common) attribute
			if (!isVersionSpec)
				srcRev = ModelVersionDBService.ALL;

			checkTableExist(LINK_TAB);
			
			// compute linkId unique for (srcId, destId, typeId)
			querySB = getBeginSelectQuery(true, LINK_TAB, LINK_LINK_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			query = getEndQuery(querySB, m_connection);
			rs = executeQuery(query);
			UUID linkId = null;
			if (rs.next()) {
				linkId = UUID.fromString(rs.getString(1));
			} else {
				// Be careful, work only if clients use same algo 
				// to generate IDs to prevent same generated IDs case
				linkId = UUID.randomUUID(); 
			}
			rs.close();

			// compute last used revision number for this link id
			int lastLinkRev = 0;
			if (linkExists(linkId))
				lastLinkRev = getLastLinkRevNb(linkId);
			int firstNewRev = lastLinkRev + 1;
			if (!isMySQL) {
				executeResetSequence(m_connection.getLastSeqName(), firstNewRev);
			}
			
			// init state map
			if (stateMap == null)
				stateMap = new HashMap<String, Object>();
			
			/*
			 *  I - Update link id and link revision number of existing links
			 */
			// 1. create temporary table (in memory) for compute new link revision numbers
			// create memory table LINK_NEW_REV_<clientId>
			// (LINK_NEW_REV INTEGER, <LINKS table columns>)
			// PRIMARY KEY LINK_NEW_REV_<clientId>_PK (LINK_ID, REVISION);
			String newRevTable = getTemporaryTable();
			querySB = getBeginCreateTableQuery(!isMySQL && !m_connection.isOracle(), isMySQL, newRevTable);
			addCreateTableColumnPart(querySB, "LINK_NEW_REV", SQLTypes.INT_SQL_TYPE_DEF, isMySQL, isMySQL);
			addColDefSeparator(querySB);
			addLinkTableColDef(querySB);
			if (!isMySQL)
				addMultiplePKPart(querySB, newRevTable + "_PK", LINK_LINK_ID_COL, LINK_REV_COL);
			querySB.append(") ");
			if (isMySQL)
				querySB.append("AUTO_INCREMENT = " + m_connection.getTypeUtil().getSQLValue(firstNewRev));
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
			
			// 2. copy links and compute new revision numbers into temporary table
			querySB = new StringBuffer();
			selectLinksToUpdate(querySB, typeId, srcId, srcRev, destId, destRev,
					new String[] { "count(*)" });
			rs = executeQuery(getEndQuery(querySB, m_connection));
			int linkToUpdateNb = 0;
			if (rs.next())
				linkToUpdateNb = rs.getInt(1);
			rs.close();
			rs = null;
			
			if (linkToUpdateNb != 0) {
				querySB = getBeginInsertQuery(newRevTable);
				List<String> columnsList = getLinkTableColumns();
				columnsList.add("LINK_NEW_REV");
				addColumnsToInsertQuery(querySB, columnsList);
				querySB.append(" (");

				if (isMySQL)
					replace("LINK_NEW_REV", "NULL", columnsList);
				else
					replace("LINK_NEW_REV", nextValueForSeq(m_connection.getLastSeqName()),
							columnsList);
				String[] columns = columnsList.toArray(new String[columnsList
						.size()]);
				selectLinksToUpdate(querySB, typeId, srcId, srcRev, destId, destRev,
						columns);
				addOrderByPart(querySB, LINK_LINK_ID_COL, LINK_REV_COL);
				querySB.append(")");
				if (m_connection.isSupportMultiRequest())
					addQuerySeparator(querySB);
				else {
					// execute simple query with MySQL
					query = getEndQuery(querySB, m_connection);
					executeUpdate(query, m_connection);
					querySB = new StringBuffer();
				}

				// 3. update link id and revision number in LINKS table
				StringBuffer newLinkRevQuerySB = getBeginSelectQuery(false,
						newRevTable, "LINK_NEW_REV");
				addWherePart(newLinkRevQuerySB);
				newLinkRevQuerySB.append("L." + LINK_LINK_ID_COL + " = "
						+ newRevTable + "." + LINK_LINK_ID_COL);
				addAndPart(newLinkRevQuerySB);
				newLinkRevQuerySB.append("L." + LINK_REV_COL + " = "
						+ newRevTable + "." + LINK_REV_COL);

				querySB.append(getBeginUpdateQuery(LINK_TAB + " L",
						new Assignment(LINK_LINK_ID_COL, linkId, m_connection.getTypeUtil()),
						new Assignment(LINK_REV_COL, "( "
								+ newLinkRevQuerySB.toString() + " )", true,
								m_connection.getTypeUtil())));
				addWherePart(querySB);
				addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
				addAndPart(querySB);
				addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
				if (srcRev != ModelVersionDBService.ALL) {
					addAndPart(querySB);
					addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
				}
				addAndPart(querySB);
				addEqualPart(querySB, LINK_DEST_ID_COL, destId);
				if (destRev != ModelVersionDBService.ALL) {
					addAndPart(querySB);
					addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
				}
				
				if (m_connection.isSupportMultiRequest()) {
					addQuerySeparator(querySB);
				} else {
					// execute simple query with MySQL
					query = getEndQuery(querySB, m_connection);
					executeUpdate(query, m_connection);
					querySB = new StringBuffer();
				}

				// 3. update link id and revision number in corresponding table
				// for this link type
				newLinkRevQuerySB = getBeginSelectQuery(false, newRevTable,
						"LINK_NEW_REV");
				addWherePart(newLinkRevQuerySB);
				newLinkRevQuerySB.append("T." + PERTYPE_OBJ_ID_COL + " = "
						+ newRevTable + "." + LINK_LINK_ID_COL);
				addAndPart(newLinkRevQuerySB);
				newLinkRevQuerySB.append("T." + PERTYPE_REV_COL + " = "
						+ newRevTable + "." + LINK_REV_COL);

				querySB.append(getBeginUpdateQuery(typeTabName + " T",
						new Assignment(PERTYPE_OBJ_ID_COL, linkId, m_connection.getTypeUtil()),
						new Assignment(PERTYPE_REV_COL, "( "
								+ newLinkRevQuerySB.toString() + " )", true,
								m_connection.getTypeUtil())));

				query = getEndQuery(querySB, m_connection);
				executeUpdate(query, m_connection);

				// 4. store new link states
				if (linkExists(linkId, firstNewRev)) {
					checkTableExist(ATTR_TAB);
					Map<String, String> attrMap = getAttributes(stateMap);
					Map<String, String> attrColMap = checkAttributes(typeId,
							attrMap, true);
					Map<String, Boolean> attrVSMap = getAttrVSFlag(typeId,
							attrMap);
					Revision revToCopy = new Revision(linkId, typeId,
							firstNewRev);
					try {
						storeState(revToCopy, typeTabName, stateMap,
								attrColMap, attrVSMap);
					} catch (IllegalArgumentException e) {
						rollbackInternalTransaction(e);
						throw new ModelVersionDBException(e);
					}

					List<Assignment> assigns = new ArrayList<Assignment>();
					for (String attr : stateMap.keySet()) {
						String colName = attrColMap.get(attr);

						StringBuffer newAttValQuerySB = getBeginSelectQuery(
								false, typeTabName + "T2", colName);
						addWherePart(newAttValQuerySB);
						addEqualPart(newAttValQuerySB, "T2."
								+ PERTYPE_OBJ_ID_COL, linkId);
						addAndPart(newAttValQuerySB);
						addEqualPart(newAttValQuerySB, "T2." + PERTYPE_REV_COL,
								firstNewRev);

						assigns.add(new Assignment("T." + colName, "( "
								+ newAttValQuerySB.toString() + " )", true,
								m_connection.getTypeUtil()));
					}

					querySB = getBeginUpdateQuery(typeTabName + " T", assigns
							.toArray(new Assignment[assigns.size()]));
					addWherePart(querySB);
					addEqualPart(querySB, "T." + PERTYPE_OBJ_ID_COL, linkId);
					addAndPart(querySB);
					querySB.append("T." + PERTYPE_REV_COL + " > "
							+ m_connection.getTypeUtil().getSQLValue(firstNewRev));
					executeUpdate(getEndQuery(querySB, m_connection), m_connection);
				}
			}
			
			// 5. delete temporary table
			executeUpdate(getDropTableQuery(newRevTable), m_connection);
			
			/*
			 *  II - Create not already existing links
			 */
			// 1. create links in LINKS table
			
			// compute last link order number from same source
			int newOrder = getLastOrder(typeId, srcId, srcRev) + 1;
			
			// get typeId and corresponding type table name of source
			UUID srcTypeId = getObjectType(srcId);
			String srcTypeTableName = getTypeTabName(srcTypeId);
			
			// get typeId and corresponding type table name of destination
			UUID destTypeId = getObjectType(destId);
			String destTypeTableName = getTypeTabName(destTypeId);
			
			// reset revision sequence number to first available one
			if (linkExists(linkId, firstNewRev))
				firstNewRev = getLastLinkRevNb(linkId) + 1;
			querySB = new StringBuffer();
			if (!isMySQL) {
				if (m_connection.isOracle())
					executeResetSequence(m_connection.getLastSeqName(), firstNewRev);
				else	
					querySB.append(getResetSequenceQuery(m_connection.getLastSeqName(), 
							firstNewRev)); // optimization for HSQLDB
			} else {
				// Workaround for MySQL because it does not manage SEQUENCE
				// create temporary table (in memory) for compute new link
				// revision numbers
				// create temporary table LINK_NEW_REV_<clientId>
				// (LINK_NEW_REV INTEGER AUTO_INCREMENT PRIMARY KEY, 
				//  SRC_ID, SRC_REV, DEST_ID, DEST_REV) 
				// AUTO_INCREMENT = <firstNewRev>;
				newRevTable = getTemporaryTable();
				querySB = getBeginCreateTableQuery(false, true,	newRevTable);
				addCreateTableColumnPart(querySB, "LINK_NEW_REV",
						SQLTypes.INT_SQL_TYPE_DEF, true, true);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, LINK_SRC_ID_COL,
						TypeUtil.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, LINK_SRC_REV_COL,
						SQLTypes.INT_SQL_TYPE_DEF, false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, LINK_DEST_ID_COL,
						TypeUtil.getVarcharDef(TypeUtil.UUID_VARCHAR_LENGTH), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, LINK_DEST_REV_COL,
						SQLTypes.INT_SQL_TYPE_DEF, false);
				querySB.append(") ");
				querySB.append("AUTO_INCREMENT = "
							+ m_connection.getTypeUtil().getSQLValue(firstNewRev));
				
				// execute simple query with MySQL
				query = getEndQuery(querySB, m_connection);
				executeUpdate(query, m_connection);
				querySB = new StringBuffer();
				
				// insert into <newRevTable> 
				// (SRC_ID, SRC_REV, DEST_ID, DEST_REV, LINK_NEW_REV)
				// (select SRC_ID, SRC_REV, DEST_ID, DEST_REV, NULL from 
				//  <join source revisions and destination revisions>)
				querySB.append(getBeginInsertQuery(newRevTable));
				List<String> columnsList = new ArrayList<String>();
				columnsList.add(LINK_SRC_ID_COL);
				columnsList.add(LINK_SRC_REV_COL);
				columnsList.add(LINK_DEST_ID_COL);
				columnsList.add(LINK_DEST_REV_COL);
				columnsList.add("LINK_NEW_REV");
				addColumnsToInsertQuery(querySB, columnsList);
				querySB.append("( ");
				querySB.append(getBeginSelectQuery(false, null, 
						LINK_SRC_ID_COL, LINK_SRC_REV_COL, 
						LINK_DEST_ID_COL, LINK_DEST_REV_COL, 
						"NULL"));
				querySB.append(getAllSrcRevJoinDestRev(srcId, srcRev, destId, destRev, 
						srcTypeTableName, destTypeTableName));
				querySB.append(") ");
				query = getEndQuery(querySB, m_connection);
				executeUpdate(query, m_connection);
				querySB = new StringBuffer();
			}
			
			// insert into PUBLIC.LINKS
			// (LINK_ID, LINK_REV, TYPE_ID, SRC_ID, SRC_REV, DEST_ID, DEST_REV, ORDER_NB)
			querySB.append(getBeginInsertQuery(LINK_TAB));
			List<String> columnsList = getLinkTableColumns();
			addColumnsToInsertQuery(querySB, columnsList);
			
			//    (select <linkId>, next value for LAST_REV_SEQ, <typeId>, SRC_ID, SRC_REV, 
			//     DEST_ID, DEST_REV, <newOrder> from
			String srcTable = "T_SRC";
			if (isMySQL)
				srcTable = newRevTable;
			String destTable = "T_DEST";
			if (isMySQL)
				destTable = newRevTable;
			
			if (!m_connection.isOracle())
				querySB.append(" (");
			columnsList = getLinkTableColumns();
			replace(LINK_LINK_ID_COL, TypeUtil.getSQLValue(linkId), columnsList);
			replace(LINK_TYPE_ID_COL, TypeUtil.getSQLValue(typeId), columnsList);
			replace(LINK_SRC_ID_COL, srcTable + "." + LINK_SRC_ID_COL, columnsList);
			replace(LINK_SRC_REV_COL, srcTable + "." + LINK_SRC_REV_COL, columnsList);
			replace(LINK_DEST_ID_COL, destTable + "." + LINK_DEST_ID_COL, columnsList);
			replace(LINK_DEST_REV_COL, destTable + "." + LINK_DEST_REV_COL, columnsList);
			if (isMySQL)
				replace(LINK_REV_COL, "LINK_NEW_REV", columnsList);
			else
				replace(LINK_REV_COL, nextValueForSeq(m_connection.getLastSeqName()), columnsList);
			replace(LINK_ORDER_COL, m_connection.getTypeUtil().getSQLValue(newOrder), columnsList);
			String[] columns = columnsList.toArray(new String[columnsList.size()]);
			querySB.append(getBeginSelectQuery(false, null, columns));
			
			if (isMySQL) {
				querySB.append(newRevTable);
			} else {
				querySB.append(getAllSrcRevJoinDestRev(srcId, srcRev, destId, destRev, 
					srcTypeTableName, destTypeTableName));
			}
			
			//where NOT EXISTS(
			addWherePart(querySB);
			querySB.append("NOT EXISTS (");
			
			//                 select * from LINKS L2
	        //                 where L2.SRC_ID = T_SRC.SRC_ID
	        //                 and L2.SRC_REV = T_SRC.SRC_REV
	        //                 and L2.DEST_ID = T_DEST.DEST_ID
	        //                 and L2.DEST_REV = T_DEST.DEST_REV
			//                 and L2.TYPE_ID = <typeId>
			querySB.append(getBeginSelectQuery(false, LINK_TAB + " L2", new String[0]));
			addWherePart(querySB);
			querySB.append("L2." + LINK_SRC_ID_COL + " = " + srcTable + "." + LINK_SRC_ID_COL);
			addAndPart(querySB);
			querySB.append("L2." + LINK_SRC_REV_COL + " = "  + srcTable + "." + LINK_SRC_REV_COL);
			addAndPart(querySB);
			querySB.append("L2." + LINK_DEST_ID_COL + " = " + destTable + "." + LINK_DEST_ID_COL);
			addAndPart(querySB);
			querySB.append("L2." + LINK_DEST_REV_COL + " = " + destTable + "." + LINK_DEST_REV_COL);
			addAndPart(querySB);
			addEqualPart(querySB, "L2." + LINK_TYPE_ID_COL, typeId);
			
            //          )
			// order by SRC_REV, DEST_REV
			// )
			querySB.append(") ");
			if (!m_connection.isOracle()) {
				addOrderByPart(querySB, LINK_SRC_REV_COL, LINK_DEST_REV_COL);
				querySB.append(")");
			}
			
			executeUpdate(getEndQuery(querySB, m_connection), m_connection);
			querySB = new StringBuffer();
			
			if (isMySQL) {
				// Workaround for MySQL
				// delete temporary table
				executeUpdate(getDropTableQuery(newRevTable), m_connection);
			}
			
			// 2. create link states in corresponding table for this link type
			if (linkExists(linkId, firstNewRev)) {
				// create first revision
				checkTableExist(ATTR_TAB);
				Map<String, String> attrMap = getAttributes(stateMap);
				Map<String, String> attrColMap = checkAttributes(typeId, attrMap, true);
				Map<String, Boolean> attrVSMap = getAttrVSFlag(typeId, attrMap);
				Revision revToCopy = new Revision(linkId, typeId, firstNewRev);
				try {
					storeState(revToCopy, typeTabName, stateMap, attrColMap, attrVSMap);
				} catch (IllegalArgumentException e) {
					rollbackInternalTransaction(e);
					throw new ModelVersionDBException(e);
				}
				
				// compute column names corresponding to attributes
				List<String> colmumns = new ArrayList<String>();
				for (String attr : stateMap.keySet()) {
					String colName = attrColMap.get(attr);
					colmumns.add(colName);
				}
			
				// insert into <typeTableName> (OBJ_ID, REVISION, <all attribute columns>) 
				// (
				List<String> colmumnsToset = new ArrayList<String>();
				colmumnsToset.addAll(colmumns);
				colmumnsToset.add(0, PERTYPE_OBJ_ID_COL);
				colmumnsToset.add(1, PERTYPE_REV_COL);
				querySB.append(getInsertQuery(typeTabName, 
						colmumnsToset.toArray(new String[colmumnsToset.size()])));
				querySB.append("( ");
				
				// select <linkId>, L2.REVISION, <all attribute columns> from <typeTableName> T,
				colmumnsToset = new ArrayList<String>();
				colmumnsToset.addAll(colmumns);
				colmumnsToset.add(0, TypeUtil.getSQLValue(linkId));
				colmumnsToset.add(1, "L2." + LINK_REV_COL);
				querySB.append(getBeginSelectQuery(false, typeTabName + " T", 
						colmumnsToset.toArray(new String[colmumnsToset.size()])));
				querySB.append(", ");
				
				// ( select LINK_REV from LINKS L
				// where L.LINK_ID = <linkId>
				// and L.LINK_REV > <first new revision>
				// ) L2
				querySB.append("( ");
				querySB.append(getBeginSelectQuery(false, LINK_TAB + " L", 
						LINK_REV_COL));
				addWherePart(querySB);
				addEqualPart(querySB, "L." + LINK_LINK_ID_COL, linkId);
				addAndPart(querySB);
				querySB.append("L." + LINK_REV_COL + " > " + m_connection.getTypeUtil().getSQLValue(firstNewRev));
				querySB.append(") L2 ");
				
				// where T.OBJ_ID = <linkId>
				// and T.REVISION = <first new revision>
				addWherePart(querySB);
				addEqualPart(querySB, "T." + PERTYPE_OBJ_ID_COL, linkId);
				addAndPart(querySB);
				addEqualPart(querySB, "T." + PERTYPE_REV_COL, firstNewRev);
				
				// );
				querySB.append(" )");
				executeUpdate(getEndQuery(querySB, m_connection), m_connection);
			}
			
			Revision rev = new Revision(linkId, typeId, getLastLinkRevNb(linkId));
			
			commitInternalTransaction();
			
			return rev;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private String nextValueForSeq(String seqName) {
		if (m_connection.isOracle())
			return m_connection.getLastSeqName() + ".NEXTVAL";
		
		return "NEXT VALUE FOR " + m_connection.getLastSeqName();
	}

	private void selectLinksToUpdate(StringBuffer querySB, final UUID typeId, final UUID srcId,
			int srcRev, final UUID destId, int destRev, 
			String[] columns) {
		querySB.append(getBeginSelectQuery(false, LINK_TAB, columns));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
		if (srcRev != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
		}
		addAndPart(querySB);
		addEqualPart(querySB, LINK_DEST_ID_COL, destId);
		if (destRev != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
		}
	}

	/**
	 * Return query which make relational join without constraint 
	 * of source revisions and destination revisions.
	 * 
	 * @param srcId
	 * @param srcRev
	 * @param destId
	 * @param destRev
	 * @param querySB
	 * @param srcTypeTableName
	 * @param destTypeTableName
	 */
	private StringBuffer getAllSrcRevJoinDestRev(final UUID srcId, int srcRev,
			final UUID destId, int destRev, 
			String srcTypeTableName, String destTypeTableName) {
		StringBuffer querySB = new StringBuffer();
		
		//        (select OBJ_ID SRC_ID, REVISION SRC_REV from <srcTypeTableName> where OBJ_ID=<srcId>) T_SRC,
		querySB.append("(");
		querySB.append(getBeginSelectQuery(false, srcTypeTableName, 
				PERTYPE_OBJ_ID_COL + " SRC_ID", PERTYPE_REV_COL + " SRC_REV"));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, srcId);
		if (srcRev != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, srcRev);
		}
		querySB.append(") T_SRC, ");
		
		//	      (select OBJ_ID DEST_ID, REVISION DEST_REV from <destTypeTableName> where OBJ_ID=<destId>) T_DEST
		querySB.append("(");
		querySB.append(getBeginSelectQuery(false, destTypeTableName, 
				PERTYPE_OBJ_ID_COL + " DEST_ID", PERTYPE_REV_COL + " DEST_REV"));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, destId);
		if (srcRev != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, destRev);
		}
		querySB.append(") T_DEST");
		
		return querySB;
	}

	private String getTemporaryTable() {
		return "LINK_NEW_REV_T_" + m_connection.getClientIdx();
	}

	private int getLastOrder(UUID typeId, UUID srcId, int srcRev)
			throws SQLException {
		StringBuffer querySB;
		String query;
		ResultSet rs;
		int lastOrder = 0;
		querySB = getBeginSelectQuery(false, LINK_TAB, maxFunction(LINK_ORDER_COL));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
		if (srcRev != ModelVersionDBService.ALL) {
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
		}
		query = getEndQuery(querySB, m_connection);
		rs = executeQuery(query);
		if (rs.next()) {
			lastOrder = rs.getInt(1);
		}
		rs.close();
		
		return lastOrder;
	}
	
	private static String getResetSequenceQuery(String lastRevSeqName, int idx) {
		StringBuffer querySB = new StringBuffer();
		querySB.append("ALTER SEQUENCE " + lastRevSeqName + " RESTART WITH " + idx);
		
		return querySB.toString();
	}
	
	public void setLinkSrcVersionSpecific(UUID typeId, boolean isVersionSpecific) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		
		beginInternalTransaction();
		
		boolean isOldVerSpec = isLinkSrcVersionSpecific(typeId);
		try {
			setLinkVersionSpecificInternal(typeId, isVersionSpecific, TYPE_LINK_SRC_VERSION_SPEC_COL);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
		
		// add links if necessary
		if (!isOldVerSpec && isVersionSpecific) {
			//TODO
		}
		
		commitInternalTransaction();
	}

	private void setLinkVersionSpecificInternal(UUID typeId,
			boolean isVersionSpecific, String versionSpecCol) throws ModelVersionDBException {
		beginInternalTransaction();
		
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			checkTypeTableExist(typeId);
			
			StringBuffer querySB = getBeginUpdateQuery(TYPE_TAB, 
					new Assignment(versionSpecCol, isVersionSpecific));	
			String query = getEndQuery(querySB, m_connection);
		    executeUpdate(query, m_connection);
		    
		    commitInternalTransaction();
		} catch (Exception e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}
	
	// make it synchronized to ensure there is no created view conflict
	public synchronized void setObjectAttVersionSpecific(UUID typeId, String attrName, 
			boolean isVersionSpecific) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkAttrNameParam(attrName);
		
		beginInternalTransaction();
		
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			
			Attribute attrJO;
			try {
				attrJO = getAttribute(typeId, attrName, true);
				
				if (attrJO.isVersionSpecific() == isVersionSpecific)
					return;

				StringBuffer querySB = getBeginUpdateQuery(ATTR_TAB,
						new Assignment(ATTR_VERSION_SPEC_COL, isVersionSpecific));
				addWherePart(querySB);
				addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
				addAndPart(querySB);
				addEqualPart(querySB, ATTR_ATTR_NAME_COL, attrName);
				String query = getEndQuery(querySB, m_connection);
				executeUpdate(query, m_connection);
				
			} catch (IllegalArgumentException e) {
				// This attribute doesn't already exist
				
				// add new attribute definition
				int lastIdx = getAttrLastIdx(typeId);
				lastIdx++;
				String newColName = getAttrColName(lastIdx);
				String attrType = m_connection.getTypeUtil().getSQLType(null); // to set type to UNDEFINED 
				
				String query = getInsertQuery(ATTR_TAB, 
						new Assignment(ATTR_TYPE_ID_COL, typeId), 
						new Assignment(ATTR_ATTR_NAME_COL, attrName),
						new Assignment(ATTR_ATTR_TYPE_COL, attrType), 
						new Assignment(ATTR_ATTR_TAB_NAME_COL, newColName),
						new Assignment(ATTR_VERSION_SPEC_COL, isVersionSpecific));
				executeUpdate(query, m_connection);
				
				// add new column in type table
				String typeTableName = checkTypeTableExist(typeId);
				if (!tableExist(typeTableName)) {
					Map<String, String> attrTotalMap = new HashMap<String, String>();
					attrTotalMap.put(newColName, attrType);
					createTypeTable(typeTableName, attrTotalMap);
				} else {
					query = getAddColumnQuery(typeTableName, newColName, attrType);
					executeUpdate(query, m_connection);
				}
			}
			
			// Copy last revision attribute values to all revisions 
			// if it is no more revision specific
			if (!isVersionSpecific && typeExistsInTypeTable(typeId)) {
				boolean isMySQL = m_connection.isMySQL();
				
				String typeTableName = getTypeTabName(typeId);
				
				String viewName = typeTableName + m_connection.getClientIdx() + "_LR";
				// create view <temp view name> as
				StringBuffer querySB = new StringBuffer();
				if (isMySQL)
					querySB.append(getBeginCreateTableQuery(viewName));
				else
					querySB.append(getCreateViewQuery(viewName));
				
				// select T.OBJ_ID, REVISION, <attribute column names> from <typeTableName> T,
				Map<String, String> attrColMap = new HashMap<String, String>();
				Map<String, String> attrTypeMap = new HashMap<String, String>();
				getAttributes(typeId, attrColMap, attrTypeMap);
				List<String> columnsList = new ArrayList<String>();
				columnsList.add("T." + PERTYPE_OBJ_ID_COL);
				columnsList.add(PERTYPE_REV_COL);
				columnsList.addAll(attrColMap.values());
				
				querySB.append(getBeginSelectQuery(false, typeTableName + " T", 
						columnsList));
				
				// , (SELECT OBJ_ID, max(REVISION) LAST_REV from <typeTableName> 
				//    GROUP BY OBJ_ID) T_LAST_REV 
				querySB.append(", (");
				querySB.append(getBeginSelectQuery(false, typeTableName, 
						PERTYPE_OBJ_ID_COL, maxFunction(PERTYPE_REV_COL) + " LAST_REV"));
				addGroupByPart(querySB, PERTYPE_OBJ_ID_COL);
				querySB.append(") T_LAST_REV ");
				
				// WHERE T.REVISION = T_LastRev.lastRev
				// AND T.OBJ_ID = T_LastRev.OBJ_ID;
				addWherePart(querySB);
				querySB.append("T." + PERTYPE_REV_COL + " = T_LAST_REV.LAST_REV");
				addAndPart(querySB);
				querySB.append("T." + PERTYPE_OBJ_ID_COL + " = T_LAST_REV." + PERTYPE_OBJ_ID_COL);
				if (isMySQL)
					querySB.append(" )");
				querySB.append(" ; ");
				querySB = workaroundMySQL(isMySQL, querySB);
				
				// UPDATE <typeTableName> SET <attribute col name> = 
				// (SELECT <attribute col name> FROM <temp view name> 
				//  WHERE <typeTableName>.OBJ_ID = <temp view name>.OBJ_ID
				// ), ...
				List<Assignment> assignsList = new ArrayList<Assignment>();
				for (String attrCol : attrColMap.values()) {
					StringBuffer attrValQuerySB = new StringBuffer();
					attrValQuerySB.append("( ");
					attrValQuerySB.append(getBeginSelectQuery(false, viewName, 
							attrCol));
					addWherePart(attrValQuerySB);
					attrValQuerySB.append(typeTableName + "." + PERTYPE_OBJ_ID_COL + " = " + viewName + "." + PERTYPE_OBJ_ID_COL);
					attrValQuerySB.append(") ");
					
					assignsList.add(new Assignment(attrCol, attrValQuerySB.toString(), true));
				}
				querySB.append(getBeginUpdateQuery(typeTableName, assignsList));
				
				
				// WHERE T_4.REVISION <> (SELECT max(REVISION) lastRev FROM <typeTableName> T
				//                        WHERE OBJ_ID = T.OBJ_ID);
				String tableForWhereClause = typeTableName;
				String lastRevCol = maxFunction(PERTYPE_REV_COL);
				if (isMySQL) {
					tableForWhereClause = viewName;
					lastRevCol = PERTYPE_REV_COL;
				}
				addWherePart(querySB);
				querySB.append(typeTableName + "." + PERTYPE_REV_COL + " <> ( ");
				querySB.append(getBeginSelectQuery(false, tableForWhereClause + " T", 
						lastRevCol));
				addWherePart(querySB);
				querySB.append("T." + PERTYPE_OBJ_ID_COL + " = " + PERTYPE_OBJ_ID_COL);
				querySB.append(" ) ;");
				querySB = workaroundMySQL(isMySQL, querySB);
				
				if (isMySQL)
					querySB.append(getDropTableQuery(viewName));
				else
					querySB.append(getDropViewQuery(viewName));
				executeUpdate(getEndQuery(querySB, m_connection), m_connection);
			}
			
			commitInternalTransaction();
		    
		} catch (Exception e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private StringBuffer workaroundMySQL(boolean isMySQL, StringBuffer querySB)
			throws SQLException {
		if (isMySQL) {
			// execute simple query with MySQL
			executeUpdate(querySB.toString(), m_connection);
			querySB = new StringBuffer();
		}
		return querySB;
	}
	
	private StringBuffer getBeginUpdateQuery(String typeTableName,
			List<Assignment> assignsList) {
		return getBeginUpdateQuery(typeTableName, 
				assignsList.toArray(new Assignment[assignsList.size()]));
	}

	private StringBuffer getBeginSelectQuery(boolean distinct, String tableName,
			List<String> columnsList) {
		return getBeginSelectQuery(distinct, tableName, columnsList.toArray(new String[columnsList.size()]));
	}

	private String getDropViewQuery(String viewName) {
		return "DROP VIEW " + viewName + " IF EXISTS;";
	}

	private StringBuffer getCreateViewQuery(String viewName) {
		StringBuffer querySB = new StringBuffer("CREATE VIEW ");
		querySB.append(viewName);
		querySB.append(" AS ");
		
		return querySB;
	}

	public int getLastObjectRevNb(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		
		// Be careful, all object revisions are contained in corresponding object type table
		// But all link revisions are contained in LINK_TAB table.
		int lastRev = FIRST_REVISION;
		ResultSet rs = null;
		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"Object with id = " + objectId + " does not exist.");

			UUID typeId = getObjectType(objectId);
			String tableName = checkTypeTableExist(typeId);
			
			StringBuffer querySB = getBeginSelectQuery(false, tableName, maxFunction(PERTYPE_REV_COL));
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB, m_connection);
			
			rs = executeQuery(query);
			if (rs.next())
				lastRev = rs.getInt(1);
				
		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return 0;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
		
		return lastRev;
	}
	
	public int getLastLinkRevNb(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException("Link with id = " + linkId + " does not exist.");
		
		// Be careful, all object revisions are contained in corresponding object type table
		// But all link revisions are contained in LINK_TAB table.
		
		StringBuffer querySB = getBeginSelectQuery(false, LINK_TAB, maxFunction(LINK_REV_COL));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
		String query = getEndQuery(querySB, m_connection);
		
		int lastRev = 0;
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (rs.next()) {
				lastRev = rs.getInt(1);
			} 
		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return 0;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
		
		return lastRev;
	}
	
	public int[] getLinkRevNbs(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException("Link with id = " + linkId + " does not exist.");
		
		// Be careful, all object revisions are contained in corresponding object type table
		// But all link revisions are contained in LINK_TAB table.
		
		StringBuffer querySB = getBeginSelectQuery(false, LINK_TAB, LINK_REV_COL);
		addWherePart(querySB);
		addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
		addOrderByPart(querySB, LINK_REV_COL);
		String query = getEndQuery(querySB, m_connection);
		
		List<Integer> revisions = new ArrayList<Integer>();
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			while (rs.next()) {
				revisions.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return new int[0];
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
		
		int[] revArray = new int[revisions.size()];
		for (int i = 0; i < revisions.size(); i++) {
			revArray[i] = revisions.get(i);
		}
		
		return revArray;
	}

	public int[] getObjectRevNbs(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		
		// Be careful, all object revisions are contained in corresponding object type table
		// But all link revisions are contained in LINK_TAB table.
		List<Integer> revisions = new ArrayList<Integer>();
		ResultSet rs = null;
		try {
			UUID typeId = getObjectType(objectId);
			String tableName = checkTypeTableExist(typeId);
			
			StringBuffer querySB = getBeginSelectQuery(false, tableName, 
					PERTYPE_REV_COL);
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			addOrderByPart(querySB, PERTYPE_REV_COL);
			String query = getEndQuery(querySB, m_connection);
			
			rs = executeQuery(query);
			while (rs.next()) {
				revisions.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return new int[0];
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
		
		int[] revArray = new int[revisions.size()];
		for (int i = 0; i < revisions.size(); i++) {
			revArray[i] = revisions.get(i);
		}
		
		return revArray;
	}

	private String maxFunction(String column) {
		return "max(" + column + ") ";
	}

	public synchronized void deleteLink(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		beginInternalTransaction();
		
		try {
			checkTableExist(LINK_TAB);

			if (!linkExists(linkId))
				throw new IllegalArgumentException("Link with id = " + linkId
						+ " doesn't exists.");

			UUID typeId = getLinkType(linkId);

			// delete object state
			String typeTabName = checkTypeTableExist(typeId);
			StringBuffer querySB = getBeginDeleteQuery(typeTabName);
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, linkId);
			String query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);

			// delete object id in objects table
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);

			// Note that we don't remove link source or destination objects

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void checkLinkIdParam(UUID linkId) {
		if (linkId == null)
			throw new IllegalArgumentException("Link id cannot be null.");
	}
	
	private void checkObjectIdParam(UUID objId) {
		if (objId == null)
			throw new IllegalArgumentException("Object id cannot be null.");
	}

	public int getLinkNumber(UUID typeId, UUID srcId, int srcRev) throws ModelVersionDBException {
		checkConnection();
		
		ResultSet rs = null;
		try {
			// manage LAST revision case
			srcRev = manageRevNb(srcId, srcRev, false);
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, 
					"count(" + LINK_DEST_ID_COL + ")");
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			if (rs.next())
				return rs.getInt(1);

			return 0;

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return 0;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public UUID getLinkDest(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");
		
		return getIdForLink(LINK_DEST_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	private UUID getIdForLink(String selColName, String equalColName,
			UUID equalId) throws ModelVersionDBException {
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, selColName);
			addWherePart(querySB);
			addEqualPart(querySB, equalColName, equalId);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			if (rs.next()) {
				return UUID.fromString(rs.getString(1));
			}
			return null;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public UUID getLinkSrc(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");
		
		return getIdForLink(LINK_SRC_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public Map<String, Object> getLinkState(UUID linkId, int rev)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkRevisionParam(rev, false, false);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");
		
		try {
			beginInternalTransaction();
			
			UUID typeId = getLinkType(linkId);
			rev = manageRevNb(linkId, rev, true);
			Map<String, Object> resultMap = getState(linkId, rev, typeId);
			commitInternalTransaction();
			
			return resultMap;
		} catch (Exception e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private int manageRevNb(UUID eltId, int rev, boolean isLink) throws ModelVersionDBException {
		if (rev != ModelVersionDBService.LAST)
			return rev;
		
		// manage last revision flag
		if (isLink)
			return getLastLinkRevNb(eltId);
		else
			return getLastObjectRevNb(eltId);
	}

	private Map<String, Object> getState(UUID objId, int rev, UUID typeId)
			throws ModelVersionDBException, SQLException {
		String tableName = checkTypeTableExist(typeId);
		Map<String, String> attrColMap = new HashMap<String, String>();
		Map<String, String> attrTypeMap = new HashMap<String, String>();
		getAttributes(typeId, attrColMap, attrTypeMap);
		
		// manage LAST revision case
		rev = manageRevNb(objId, rev, false);

		// get values in DB
		StringBuffer querySB = getBeginSelectQuery(false, tableName, "*");
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objId);
		addAndPart(querySB);
		addEqualPart(querySB, PERTYPE_REV_COL, rev);
		String query = getEndQuery(querySB, m_connection);
		
		Map<String, Object> resultMap = new HashMap<String, Object>();
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (!rs.next())
				return resultMap;

			for (String attr : attrTypeMap.keySet()) {
				String colName = attrColMap.get(attr);
				String attrType = attrTypeMap.get(attr);
				Object value = getValue(rs, colName, attrType);

				resultMap.put(attr, value);
			}
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}

		return resultMap;
	}

	private Object getValue(ResultSet rs, String colName, String type)
			throws SQLException, ModelVersionDBException {

		if (rs.findColumn(colName) <= 0) {
			// not found
			throw new IllegalArgumentException("Cannot find column " + colName);
		}
		
		rs.getObject(colName); // just for call wasNull() method
		if (rs.wasNull())
			return null;
		
		if (TypeUtil.isBoolean(type)) {
			return getBoolean(rs, colName);
		}
		
		if (TypeUtil.isUUID(type))
			return UUID.fromString(rs.getString(colName));

		// must be after UUID type recognizer
		if (TypeUtil.isVarchar(type))
			return rs.getString(colName);

		if (m_connection.getTypeUtil().isSerType(type))
			return getSerializableObject(rs, colName);

		if (TypeUtil.isInt(type))
			return new Integer(rs.getInt(colName));
		
		if (TypeUtil.isJavaDate(type))
			return new java.util.Date(rs.getLong(colName));

		if (TypeUtil.isLong(type))
			return new Long(rs.getLong(colName));

		if (TypeUtil.isSQLTime(type))
			return rs.getTime(colName);

		if (TypeUtil.isSQLDate(type))
			return rs.getDate(colName);

		return rs.getObject(colName);
	}

	private boolean getBoolean(ResultSet rs, String colName) throws SQLException {
		if (!m_connection.isOracle())
			return rs.getBoolean(colName);
		
		String str = rs.getString(colName);
		if ("T".equals(str))
			return Boolean.TRUE;
		else
			return Boolean.FALSE;
	}

	private Object getSerializableObject(ResultSet rs, String column)
			throws ModelVersionDBException, SQLException {
		byte[] value = rs.getBytes(column);
		if (value == null)
			return null;
		
		ByteArrayInputStream bais = new ByteArrayInputStream(value);
		Object obj = null;
		try {
			ObjectInputStream ois = new ObjectInputStream(bais);
			obj = ois.readObject();
			ois.close();
			bais.close();
		} catch (Exception e) {
			throw new ModelVersionDBException(e);
		}

		return obj;
	}

	private void getAttributes(UUID typeId, Map<String, String> attrColMap,
			Map<String, String> attrTypeMap) throws SQLException {
		// get attributes in DB
		StringBuffer querySB = getBeginSelectQuery(false, ATTR_TAB,
				ATTR_ATTR_NAME_COL, ATTR_ATTR_TYPE_COL, ATTR_ATTR_TAB_NAME_COL);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = null;
		try {
			rs = executeQuery(query);

			while (rs.next()) {
				String attr = rs.getString(ATTR_ATTR_NAME_COL);
				String attrType = rs.getString(ATTR_ATTR_TYPE_COL);
				String colName = rs.getString(ATTR_ATTR_TAB_NAME_COL);
				attrColMap.put(attr, colName);
				attrTypeMap.put(attr, attrType);
			}
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public UUID getLinkType(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");
		
		return getIdForLink(LINK_TYPE_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public Object getLinkValue(UUID linkId, int rev, String attrName)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkAttrNameParam(attrName);
		checkRevisionParam(rev, false, false);
		beginInternalTransaction();
		
		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			UUID typeId = getLinkType(linkId);
			rev = manageRevNb(linkId, rev, true);
			Object result = getValue(linkId, rev, typeId, attrName);
			commitInternalTransaction();
			return result;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	/**
	 * @param attrName
	 */
	private void checkAttrNameParam(String attrName) {
		if (attrName == null)
			throw new IllegalArgumentException("Attribute name cannot be null.");
	}

	private Object getValue(UUID objId, int rev, UUID typeId, String attr)
			throws ModelVersionDBException, SQLException {
		String tableName = checkTypeTableExist(typeId);
		Attribute attrJO = getAttribute(typeId, attr, true);
		
		// manage LAST revision case
		rev = manageRevNb(objId, rev, false);

		// get values in DB
		StringBuffer querySB = getBeginSelectQuery(false, tableName, attrJO
				.getColumn());
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objId);
		addAndPart(querySB);
		addEqualPart(querySB, PERTYPE_REV_COL, rev);
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (!rs.next())
				throw new IllegalArgumentException("Cannot find attribute " + attr);
		
			return getValue(rs, attrJO.getColumn(), attrJO.getType());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Cannot find attribute " + attr);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	private Attribute getAttribute(UUID typeId, String attr, boolean throwExceptionIfNoAttrDef)
			throws ModelVersionDBException, SQLException {
		// get attributes in DB
		checkTableExist(ATTR_TAB);
		StringBuffer querySB = getBeginSelectQuery(false, ATTR_TAB,
				ATTR_ATTR_TYPE_COL, ATTR_ATTR_TAB_NAME_COL, ATTR_VERSION_SPEC_COL);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, ATTR_ATTR_NAME_COL, attr);
		String query = getEndQuery(querySB, m_connection);
		String attrType;
		String colName;
		boolean isVersionSpec;
		ResultSet rs = null;
		try {
			rs = executeQuery(query);

			if (!rs.next()) {
				if (throwExceptionIfNoAttrDef) {
					throw new IllegalArgumentException("Cannot find attribute " + attr
						+ " definition for object type id = " + typeId);
				} else {
					return null;
				}
			}
				
			attrType = rs.getString(ATTR_ATTR_TYPE_COL);
			colName = rs.getString(ATTR_ATTR_TAB_NAME_COL);
			isVersionSpec = getBoolean(rs, ATTR_VERSION_SPEC_COL);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}

		return new Attribute(attrType, colName, isVersionSpec);
	}

	public Set<UUID> getLinks() throws ModelVersionDBException {
		return new HashSet<UUID>(getIdsForLink(LINK_LINK_ID_COL, null, null, null, null));
	}

	public synchronized Map<String, Object> getObjectState(UUID objectId, int rev)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkRevisionParam(rev, false, false);
		
		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");
			if (!objExists(objectId, rev))
				throw new IllegalArgumentException(
						"There is no revision " + rev + " for object " + objectId
								+ " in database");
			
			beginInternalTransaction();

			UUID typeId = getObjectType(objectId);
			rev = manageRevNb(objectId, rev, false);
			Map<String, Object> resultMap = getState(objectId, rev, typeId);
			
			commitInternalTransaction();
			
			
			
			return resultMap;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public Object getObjectValue(UUID objectId, int rev, String attrName)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkAttrNameParam(attrName);
		checkRevisionParam(rev, false, false);
		beginInternalTransaction();
		
		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");

			UUID typeId = getObjectType(objectId);
			rev = manageRevNb(objectId, rev, false);
			Object value = getValue(objectId, rev, typeId, attrName);
			commitInternalTransaction();
			
			return value;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public boolean linkExists(UUID linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		
		ResultSet rs = null;
		try {
			if (!tableExist(LINK_TAB))
				return false;
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, "*");
			addWherePart(querySB);
			addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			return (rs.next());

		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return false;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}
	
	public boolean linkExists(UUID linkId, int rev)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkRevisionParam(rev, false, true);
		if ((rev == ModelVersionDBService.ANY) || (rev == ModelVersionDBService.LAST))
			return linkExists(linkId);
		
		ResultSet rs = null;
		try {
			if (!tableExist(LINK_TAB))
				return false;
			
			// manage LAST revision case
			rev = manageRevNb(linkId, rev, true);
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_REV_COL, rev);
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			return (rs.next());

		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return false;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public boolean linkExists(UUID typeId, UUID srcId, int srcRev, UUID destId, int destRev)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkRevisionParam(srcRev, false, true);
		checkDestIdParam(destId);
		checkRevisionParam(destRev, false, true);
		
		ResultSet rs = null;
		try {
			srcRev = manageRevNb(srcId, srcRev, false);
			destRev = manageRevNb(destId, destRev, false);
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, "count(*)");
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			if (srcRev != ModelVersionDBService.ANY) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			}
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			if (destRev != ModelVersionDBService.ANY) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
			}
			String query = getEndQuery(querySB, m_connection);

			rs = executeQuery(query);

			if (!rs.next())
				return false;

			return (rs.getInt(1) > 0);

		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return false;
			
			throw new ModelVersionDBException(e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
	}

	public synchronized void setLinkState(UUID linkId, int rev, Map<String, Object> stateMap)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkRevisionParam(rev, true, false);
		
		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			if ((stateMap == null) || (stateMap.isEmpty()))
				throw new IllegalArgumentException(
						"State map musn't be null or empty");
			
			UUID typeId = getLinkType(linkId);
			
			beginInternalTransaction();
			
			String typeTabName = checkTypeTableExist(typeId);

			Revision revision= new Revision(linkId, typeId, manageRevNb(linkId, rev, true));
			
			Map<String, String> attrMap = getAttributes(stateMap);
			Map<String, String> attrColMap = checkAttributes(typeId, attrMap, true);
			Map<String, Boolean> attrVSMap = getAttrVSFlag(typeId, attrMap);

			try {
				storeState(revision, typeTabName, stateMap, attrColMap, attrVSMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void setLinkValue(UUID linkId, int rev, String attrName, Object value)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkAttrNameParam(attrName);
		
		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			UUID typeId = getLinkType(linkId);
			
			beginInternalTransaction();
			
			String typeTabName = checkTypeTableExist(typeId);
			
			Revision revision = new Revision(linkId, typeId, manageRevNb(linkId, rev, true));

			updateValue(revision, attrName, value, typeTabName);
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void updateValue(Revision rev, String attrName, Object value,
			String typeTabName) throws ModelVersionDBException,
			SQLException {
		
		UUID typeId = rev.getTypeId();
		
		Attribute attrJO;
		try {
			attrJO = getAttribute(typeId, attrName, true);
		} catch (IllegalArgumentException e) {
			// This attribute doesn't already exist
			
			// add new attribute definition
			
			int lastIdx = getAttrLastIdx(typeId);
			lastIdx++;
			String newColName = getAttrColName(lastIdx);
			String attrType = m_connection.getTypeUtil().getSQLType(value);
			
			Boolean attrVSFlag = DEFAULT_VERSION_SPECIFIC_FLAG;
			String query = getInsertQuery(ATTR_TAB, 
					new Assignment(ATTR_TYPE_ID_COL, typeId), 
					new Assignment(ATTR_ATTR_NAME_COL, attrName),
					new Assignment(ATTR_ATTR_TYPE_COL, attrType), 
					new Assignment(ATTR_ATTR_TAB_NAME_COL, newColName),
					new Assignment(ATTR_VERSION_SPEC_COL, attrVSFlag));
			executeUpdate(query, m_connection);
			
			// add new column in type table
			query = getAddColumnQuery(getTypeTabName(typeId), newColName, attrType);
			executeUpdate(query, m_connection);
			
			attrJO = new Attribute(attrType, newColName, attrVSFlag);
		}

		// check attribute exist
		String newAttrType = null;
		try {
			newAttrType = m_connection.getTypeUtil().getSQLType(value);
		} catch (ModelVersionDBException e) {
			throw new ModelVersionDBException("Attribute " + attrName
					+ " has no serializable type.", e);
		}
		String attrType = attrJO.getType();
		if (m_connection.getTypeUtil().incompatibleTypes(attrType, newAttrType)) {
			if (m_connection.getTypeUtil().migratableToNewType(attrType, newAttrType)) {
				int lastIdx = getAttrLastIdx(typeId);
				lastIdx++;
				String newColName = getAttrColName(lastIdx);
				migrateCol(typeId, newAttrType, newColName, attrJO.getColumn());
				attrJO.setColumn(newColName);
			} else
				throw new ModelVersionDBException("Found persist type "
					+ attrType + " and new type " + newAttrType
					+ " for attribute " + attrName + " of type " + typeId);
		}
		
		// update attribute value in DB
		StringBuffer querySB = getBeginUpdateQuery(typeTabName, new Assignment(attrJO.getColumn()));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, rev.getId());
		if ((rev.getRev() != ModelVersionDBService.ALL) && attrJO.isVersionSpecific()) {
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, rev.getRev());
		}
		String query = getEndQuery(querySB, m_connection);
		PStatement ps = createPreparedStatement(query);
		try {
			setValue(ps, 1, value);
		} catch (IllegalArgumentException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
		try {
			ps.executeUpdate();
			ps.close();
		} catch (IOException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private int getAttrLastIdx(UUID typeId) throws SQLException {
		int lastIdx = -1;
		
		StringBuffer querySB = getBeginSelectQuery(false, ATTR_TAB,
				ATTR_ATTR_NAME_COL, ATTR_ATTR_TYPE_COL, ATTR_ATTR_TAB_NAME_COL);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			while (rs.next()) {
				String colName = rs.getString(ATTR_ATTR_TAB_NAME_COL);
				Integer idx = getAttrIdx(colName);
				if (idx > lastIdx)
					lastIdx = idx;
			}
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore it
				}
		}
		
		return lastIdx;
	}

	public synchronized void setObjectState(UUID objectId, int rev, Map<String, Object> stateMap)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkRevisionParam(rev, true, false);
		
		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");

			if ((stateMap == null) || (stateMap.isEmpty()))
				throw new IllegalArgumentException(
						"State map musn't be null or empty");
			
			if ((rev != ModelVersionDBService.ALL) && !objExists(objectId, rev))
				throw new IllegalArgumentException(
						"There is no revision " + rev + " for object " + objectId
								+ " in database");
			
			UUID typeId = getObjectType(objectId);
			
			beginInternalTransaction();
			
			String typeTabName = checkTypeTableExist(typeId);

			Map<String, String> attrMap = getAttributes(stateMap);
			Map<String, String> attrColMap = checkAttributes(typeId, attrMap, true);
			Map<String, Boolean> attrVSMap = getAttrVSFlag(typeId, attrMap);

			Revision revision = new Revision(objectId, typeId, manageRevNb(objectId, rev, false));
			
			try {
				storeState(revision, typeTabName, stateMap, attrColMap, attrVSMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private Map<String, Boolean> getAttrVSFlag(UUID typeId,
			Map<String, String> attrMap) throws SQLException, ModelVersionDBException {
		// TODO should refactor to retrieve these flags with attribute column name
		
		StringBuffer querySB = getBeginSelectQuery(false, ATTR_TAB,
				ATTR_ATTR_NAME_COL, ATTR_VERSION_SPEC_COL);
		addWherePart(querySB);
		addEqualPart(querySB, ATTR_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);
		ResultSet rs = executeQuery(query);
		
		Map<String, Boolean> attrVSMap = new HashMap<String, Boolean>();
		while (rs.next()) {
			String attr = rs.getString(ATTR_ATTR_NAME_COL);
			Boolean attrVSFlag = getBoolean(rs, ATTR_VERSION_SPEC_COL);
			if (attrVSFlag == null)
				throw new ModelVersionDBException(
						"Unexpected error : version specific flag for attribute " + 
						attr + " is not specified.");
			
			attrVSMap.put(attr, attrVSFlag);
		}
		
		return attrVSMap;
	}

	public synchronized void setObjectValue(UUID objectId, int rev, String attrName, Object value)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkAttrNameParam(attrName);
		checkRevisionParam(rev, true, false);
		
		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId + " in database");
			
			if ((rev != ModelVersionDBService.ALL) && !objExists(objectId, rev))
				throw new IllegalArgumentException(
						"There is no revision " + rev + " for object " + objectId
								+ " in database");

			UUID typeId = getObjectType(objectId);
			
			beginInternalTransaction();
			
			String typeTabName = checkTypeTableExist(typeId);
			
			Revision revision = new Revision(objectId, typeId, manageRevNb(objectId, rev, false));

			updateValue(revision, attrName, value, typeTabName);
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public List<Revision> getLinkDestRev(UUID linkTypeId, UUID srcId, int srcRev)
			throws ModelVersionDBException {
		checkConnection();
		checkSrcIdParam(srcId);
		checkTypeParam(linkTypeId);
		checkRevisionParam(srcRev, true, false);
		
		// manage LAST revision case
		srcRev = manageRevNb(srcId, srcRev, false);
		
		return getRevisionsForLink(LINK_DEST_ID_COL, LINK_DEST_REV_COL, linkTypeId,
				LINK_SRC_ID_COL, srcId, LINK_SRC_REV_COL, srcRev);
	}
	
	public Revision getLinkRev(UUID linkTypeId, UUID srcId, int srcRev, 
			UUID destId, int destRev) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(linkTypeId);
		checkSrcIdParam(srcId);
		checkRevisionParam(srcRev, false, false);
		checkDestIdParam(destId);
		checkRevisionParam(destRev, false, false);
		
		try {
			if (!tableExist(LINK_TAB))
				return null;
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, 
					LINK_LINK_ID_COL, LINK_REV_COL);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, linkTypeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			while (rs.next()) {
				UUID idToGet = UUID.fromString(rs.getString(1));
				return new Revision(idToGet, linkTypeId, rs.getInt(2));
			}
			return null;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private List<Revision> getRevisionsForLink(
			String idToGetCol, String revToGetCol, 
			UUID linkTypeId, String idCol, UUID id, 
			String revCol, int rev) throws ModelVersionDBException {
		try {
			if (!tableExist(LINK_TAB))
				return new ArrayList<Revision>();
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, 
					idToGetCol, revToGetCol, LINK_SRC_ID_COL, LINK_ORDER_COL);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, linkTypeId);
			addAndPart(querySB);
			addEqualPart(querySB, idCol, id);
			if ((revCol != null) && (rev >= FIRST_REVISION)) {
				addAndPart(querySB);
				addEqualPart(querySB, revCol, rev);
			}
			addOrderByPart(querySB, LINK_SRC_ID_COL, LINK_ORDER_COL);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			List<Revision> revisions = new ArrayList<Revision>();
			while (rs.next()) {
				UUID idToGet = UUID.fromString(rs.getString(1));
				revisions.add(new Revision(idToGet, getObjectType(idToGet), rs.getInt(2)));
			}
			return revisions;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	public List<Revision> getLinkSrcRev(UUID linkTypeId, UUID destId, int destRev)
			throws ModelVersionDBException {
		checkConnection();
		checkDestIdParam(destId);
		checkTypeParam(linkTypeId);
		checkRevisionParam(destRev, true, false);
		
		// manage LAST revision case
		destRev = manageRevNb(destId, destRev, false);
		
		return getRevisionsForLink(LINK_SRC_ID_COL, LINK_SRC_REV_COL, linkTypeId,
				LINK_DEST_ID_COL, destId, LINK_DEST_REV_COL, destRev);
	}

	private List<UUID> getIdsForLink(String selColName, String fEqualColName,
			UUID fEqualId, String sEqualColName, UUID sEqualId)
			throws ModelVersionDBException {
		checkConnection();
		
		try {
			if (!tableExist(LINK_TAB))
				return new ArrayList<UUID>();
			
			List<String> columns = new ArrayList<String>();
			columns.add(selColName);
			if (m_connection.isHSQL()) {
				columns.add(LINK_SRC_ID_COL);
				columns.add(LINK_TYPE_ID_COL);
				columns.add(LINK_ORDER_COL);
			}
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, columns);
			if ((fEqualColName != null) || (sEqualColName != null)) {
				addWherePart(querySB);
				addEqualPart(querySB, fEqualColName, fEqualId);
				addAndPart(querySB);
				addEqualPart(querySB, sEqualColName, sEqualId);
			}
			addOrderByPart(querySB, LINK_SRC_ID_COL, LINK_TYPE_ID_COL, LINK_ORDER_COL);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			List<UUID> ids = new ArrayList<UUID>();
			while (rs.next()) {
				ids.add(UUID.fromString(rs.getString(1)));
			}
			return ids;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	public List<UUID> getLinks(UUID typeId) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		
		try {
			if (!tableExist(LINK_TAB))
				return new ArrayList<UUID>();
			
			List<String> columns = new ArrayList<String>();
			columns.add(LINK_LINK_ID_COL);
			if (m_connection.isHSQL()) {
				columns.add(LINK_SRC_ID_COL);
				columns.add(LINK_ORDER_COL);
			}
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, columns);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addOrderByPart(querySB, LINK_SRC_ID_COL, LINK_ORDER_COL);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			List<UUID> results = new ArrayList<UUID>();
			while (rs.next()) {
				results.add(UUID.fromString(rs.getString(1)));
			}
			return results;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void addOrderByPart(StringBuffer querySB, String... columns) {
		if ((columns == null) || (columns.length == 0))
			return;
		
		querySB.append(" ORDER BY ");
		for (int i = 0; i < columns.length; i++) {
			if (i > 0)
				querySB.append(", ");
			querySB.append(columns[i]);
		}
	}
	
	private void addGroupByPart(StringBuffer querySB, String... columns) {
		if ((columns == null) || (columns.length == 0))
			return;
		
		querySB.append(" GROUP BY ");
		for (int i = 0; i < columns.length; i++) {
			if (i > 0)
				querySB.append(",");
			querySB.append(columns[i]);
		}
	}

	// should remove synchronized but there is an error
	public synchronized List<Revision> getObjectRevs(UUID typeId, Map<String, Object> stateMap, boolean onlyLastRev)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		beginInternalTransaction();
		
		try {
			List<Revision> results = new ArrayList<Revision>();
			
			checkTableExist(TYPE_TAB);
			if (!typeExistsInTypeTable(typeId)) {
				commitInternalTransaction();
				return results;
			}
			
			// retrieve attribute definition
			String tableName = checkTypeTableExist(typeId);
			Map<String, String> attrColMap = new HashMap<String, String>();
			Map<String, String> attrTypeMap = new HashMap<String, String>();
			getAttributes(typeId, attrColMap, attrTypeMap);
			Map<String, String> attrMap = getAttributes(stateMap);
			checkAttributes(typeId, attrMap, false);
			
			boolean definitionNotFound = false;
			for (String attr : attrMap.keySet()) {
				if (!attrTypeMap.containsKey(attr))
					definitionNotFound = true;
			}
			if (definitionNotFound) {
				return results; // at least one attribute is not defined for this type
			}

			// get all columns of serializable type
			List<String> columnsToGet = new ArrayList<String>();
			columnsToGet.add(PERTYPE_OBJ_ID_COL);
			columnsToGet.add(PERTYPE_REV_COL);
			List<String> serAttrList = new ArrayList<String>();
			for (Iterator<String> it = attrMap.keySet().iterator(); it.hasNext(); ) {
				String attr = it.next();
				String type = attrTypeMap.get(attr);
				if (m_connection.getTypeUtil().isSerType(type)) {
					String colName = attrColMap.get(attr);
					columnsToGet.add(colName);
					serAttrList.add(attr);
				}
			}
			
			// get objects in DB
			StringBuffer querySB = getBeginSelectQuery(!onlyLastRev, tableName, columnsToGet.toArray(new String[columnsToGet.size()]));
			if (onlyLastRev && serAttrList.isEmpty()) {
				querySB.append(", (");
				querySB.append(getBeginSelectQuery(false, tableName, 
						PERTYPE_OBJ_ID_COL, maxFunction(PERTYPE_REV_COL) + " LAST_REV"));
				addNotSerAttrEquals(stateMap, attrColMap, attrTypeMap, attrMap,
						querySB);
				addGroupByPart(querySB, PERTYPE_OBJ_ID_COL);
				querySB.append(") T_LAST_REV");
			}
			if (!attrMap.isEmpty() && (attrMap.size() != serAttrList.size())) {
				addNotSerAttrEquals(stateMap, attrColMap, attrTypeMap, attrMap,
						querySB);
			}
			if (onlyLastRev && serAttrList.isEmpty()) {
				addAndPart(querySB);
				querySB.append("T_LAST_REV.LAST_REV = " + tableName + "." + PERTYPE_REV_COL);
				addOrderByPart(querySB, PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL);
			} else {
				addOrderByPart(querySB, PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL);
			}
			String query = getEndQuery(querySB, m_connection);
			ResultSet rs = executeQuery(query);

			while (rs.next()) {
				boolean match = true;
				for (String attr : serAttrList) {
					String colName = attrColMap.get(attr);
					Object serValue = getSerializableObject(rs, colName);
					if (!sameValues(stateMap.get(attr), serValue)) {
						match = false;
						break;
					}
				}
				
				if (match) {
					UUID objId = UUID.fromString(rs.getString(1));
					int rev = rs.getInt(2);
					results.add(new Revision(objId, typeId, rev));
				}
			}
			
			if (onlyLastRev && !serAttrList.isEmpty()) {
				keepOnlyLastRev(results);
			}
			
			commitInternalTransaction();
			
			return results;
		} catch (Exception e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void addNotSerAttrEquals(Map<String, Object> stateMap,
			Map<String, String> attrColMap, Map<String, String> attrTypeMap,
			Map<String, String> attrMap, StringBuffer querySB)
			throws ModelVersionDBException {
		addWherePart(querySB);
		for (Iterator<String> it = attrMap.keySet().iterator(); it.hasNext(); ) {
			String attr = it.next();
			String colName = attrColMap.get(attr);
			String type = attrTypeMap.get(attr);
			if ((colName == null) || (m_connection.getTypeUtil().isSerType(type))) {
				if (!it.hasNext()) {
					removeAndPart(querySB);
				}
				continue;
			}
			
			Object value = stateMap.get(attr);
			String valueType = m_connection.getTypeUtil().getSQLType(value);
			if (!TypeUtil.sameBasicTypes(valueType, type))
				throw new ModelVersionDBException(value + " for attribute " + attr + " is an invalid value for type " + type + ".");
			
			addEqualPart(querySB, colName, value);
			
			if (it.hasNext()) {
				addAndPart(querySB);
			}
		}
	}

	private void checkTypeParam(UUID typeId) {
		if (typeId == null)
			throw new IllegalArgumentException("Type id cannot be null.");
	}

	private static void removeAndPart(StringBuffer querySB) {
		int andLength = SQLConstants.AND_PART.length();
		int andPartIdx = querySB.length() - andLength;
		querySB.delete(andPartIdx, querySB.length());
	}

	public List<Revision> getObjectRevs(UUID typeId, String attrName, Object value, boolean onlyLastRev)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkAttrNameParam(attrName);
		beginInternalTransaction();
		
		try {
			List<Revision> results = new ArrayList<Revision>();
			
			// retrieve attribute definition
			checkTableExist(TYPE_TAB);
			if (!typeExistsInTypeTable(typeId)) {
				commitInternalTransaction();
				return results;
			}
			
			String tableName = checkTypeTableExist(typeId);
			Attribute attrJO = getAttribute(typeId, attrName, false);
			if (attrJO == null) {
				commitInternalTransaction();
				
				return results;
			}
			if (m_connection.getTypeUtil().incompatibleTypes(attrJO.getType(), 
					m_connection.getTypeUtil().getSQLType(value))) {
				commitInternalTransaction();
				
				throw new ModelVersionDBException("Value " + value + 
						" is incompatible with attribute type " + 
						attrJO.getType() + " of attribute " + attrName + ".");
			}
			
			if (m_connection.getTypeUtil().isSerType(attrJO.getType())) {
				// get objects in DB
				String attrCol = attrJO.getColumn();
				StringBuffer querySB = getBeginSelectQuery(true, tableName, 
						PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL, attrCol);
				String query = getEndQuery(querySB, m_connection);
				ResultSet rs = executeQuery(query);
				while (rs.next()) {
					Object attrVal = getSerializableObject(rs, attrCol);
					if (sameValues(attrVal, value)) {
						UUID objId = UUID.fromString(rs.getString(PERTYPE_OBJ_ID_COL));
						int rev = rs.getInt(PERTYPE_REV_COL);
						results.add(new Revision(objId, typeId, rev));
					}
				}
				
				if (onlyLastRev) {
					keepOnlyLastRev(results);
				}

				commitInternalTransaction();
				
				return results;
			}

			// get objects in DB
			StringBuffer querySB = getBeginSelectQuery(true, tableName, 
					tableName + "." + PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL);
			if (onlyLastRev) {
				querySB.append(", (");
				querySB.append(getBeginSelectQuery(false, tableName, 
						PERTYPE_OBJ_ID_COL, maxFunction(PERTYPE_REV_COL) + " LAST_REV"));
				addWherePart(querySB);
				addEqualPart(querySB, attrJO.getColumn(), value);
				addGroupByPart(querySB, PERTYPE_OBJ_ID_COL);
				querySB.append(") T_LAST_REV");
			}
			addWherePart(querySB);
			addEqualPart(querySB, attrJO
					.getColumn(), value);
			if (onlyLastRev) {
				addAndPart(querySB);
				querySB.append("T_LAST_REV." + PERTYPE_OBJ_ID_COL + " = " + tableName + "." + PERTYPE_OBJ_ID_COL);
				addAndPart(querySB);
				querySB.append("T_LAST_REV.LAST_REV = " + tableName + "." + PERTYPE_REV_COL);
			}
			addOrderByPart(querySB, PERTYPE_OBJ_ID_COL, PERTYPE_REV_COL);
			String query = getEndQuery(querySB, m_connection);
			ResultSet rs = executeQuery(query);

			while (rs.next()) {
				UUID objId = UUID.fromString(rs.getString(PERTYPE_OBJ_ID_COL));
				int rev = rs.getInt(PERTYPE_REV_COL);
				results.add(new Revision(objId, typeId, rev));
			}
			
			commitInternalTransaction();
			
			return results;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void keepOnlyLastRev(List<Revision> results) {
		for (ListIterator<Revision> it = results.listIterator(); it.hasNext(); ) {
			Revision rev = it.next();
			if (!it.hasNext())
				break;
			
			Revision nextRev = it.next();
			if (!nextRev.getId().equals(rev.getId())) {
				it.previous();
				continue;
			}
			
			it.previous();
			it.previous();
			it.remove();
		}
	}

	private boolean sameValues(Object firstVal, Object secondVal) {
		if (firstVal == null)
			return (secondVal == null);
		
		return firstVal.equals(secondVal);
	}

	public UUID getOutgoingLink(UUID typeId, UUID srcId, int srcRev, UUID destId)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkDestIdParam(destId);
		checkRevisionParam(srcRev, false, false);
		
		try {
			if (!tableExist(LINK_TAB)) {
				return null;
			}
			
			// manage LAST revision case
			srcRev = manageRevNb(srcId, srcRev, false);
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, LINK_LINK_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);
			
			if (rs.next()) {
				return UUID.fromString(rs.getString(1));
			} else {
				return null;
			}

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void checkDestIdParam(UUID destId) {
		if (destId == null)
			throw new IllegalArgumentException("Source object id cannot be null.");
	}

	private void checkSrcIdParam(UUID srcId) {
		if (srcId == null)
			throw new IllegalArgumentException("Destination object id cannot be null.");
	}
	
	public List<Revision> getOutgoingLinks(UUID srcId, int srcRev, UUID destId)
			throws ModelVersionDBException {
		checkDestIdParam(destId);

		return getOutgoingLinksInternal(null, srcId, srcRev, destId);
	}
	
	private Set<UUID> getTypes() throws ModelVersionDBException {
		try {
			if (!tableExist(TYPE_TAB))
				return new HashSet<UUID>();
			
			StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB, TYPE_TYPE_ID_COL);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			Set<UUID> results = new HashSet<UUID>();
			while (rs.next()) {
				results.add(UUID.fromString(rs.getString(1)));
			}
			return results;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	public synchronized void clear() throws ModelVersionDBException {
		checkConnection();
		beginInternalTransaction();
		
		if (!m_allowAdmin)
			throw new SecurityException("Administration of this service is not allowed !");
		
		try {
			if (!tableExist(TYPE_TAB))
				return;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
		
		for (UUID type : getTypes()) {
			String typeTableName = getTypeTabName(type);
			try {
				deleteTable(typeTableName);
			} catch (SQLException e) {
				m_logger.log(Logger.ERROR, "Delete table " + typeTableName + " failed.", e);
			}
		}
		
		forceDeleteTable(OBJ_TAB);
		forceDeleteTable(GEN_TAB);
		forceDeleteTable(TYPE_TAB);
		forceDeleteTable(ATTR_TAB);
		forceDeleteTable(LINK_TAB);
		forceDeleteTable(getTemporaryTable());
		
		commitInternalTransaction();
	}

	private void forceDeleteTable(String table) {
		try {
			if (tableExist(table))
				deleteTable(table);
		} catch (SQLException e) {
			m_logger.log(Logger.ERROR, "Delete table " + table + " failed.", e);
		}
	}

	private void checkConnection() throws ModelVersionDBException {
		if (!isConnected())
			throw new ModelVersionDBException("No connection has been opened.");
	}

	public List<Revision> getOutgoingLinks(UUID typeId, UUID srcId, int srcRev)
			throws ModelVersionDBException {
		checkTypeParam(typeId);
		
		return getOutgoingLinksInternal(typeId, srcId, srcRev, null);
	}

	/**
	 * if typeId is null, consider all link types.
	 * 
	 * @param typeId
	 * @param srcId
	 * @param srcRev
	 * @param destId
	 * @return
	 * @throws ModelVersionDBException
	 */
	private List<Revision> getOutgoingLinksInternal(UUID typeId, UUID srcId,
			int srcRev, UUID destId) throws ModelVersionDBException {
		checkConnection();
		
		checkSrcIdParam(srcId);
		checkRevisionParam(srcRev, true, false);
		
		try {
			if (!tableExist(LINK_TAB))
				return new ArrayList<Revision>();
			
			// manage LAST revision case
			srcRev = manageRevNb(srcId, srcRev, false);
			
			List<String> columns = new ArrayList<String>();
			columns.add(LINK_LINK_ID_COL);
			columns.add(LINK_REV_COL);
			if (m_connection.isHSQL()) {
				columns.add(LINK_SRC_REV_COL);
				columns.add(LINK_ORDER_COL);
			}
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB);
			addWherePart(querySB);
			if (typeId != null) {
				addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
				addAndPart(querySB);
			}
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			if (srcRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			} 
			addOrderByPart(querySB, LINK_SRC_REV_COL, LINK_ORDER_COL);
			
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);

			List<Revision> results = new ArrayList<Revision>();
			while (rs.next()) {
				UUID linkId = UUID.fromString(rs.getString(LINK_LINK_ID_COL));
				int linkRev = rs.getInt(LINK_REV_COL);
				UUID curTypeId = (typeId == null) ? getLinkType(linkId) : typeId;
				results.add(new Revision(linkId, curTypeId, linkRev));
			}
			return results;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}
	
	public UUID getLinkId(UUID typeId, UUID srcId, int srcRev, UUID destId)
			throws ModelVersionDBException {
		checkConnection();
		checkDestIdParam(destId);
		checkSrcIdParam(srcId);
		checkTypeParam(typeId);
		checkRevisionParam(srcRev, false, false);
		
		try {
			if (!tableExist(LINK_TAB))
				return null;
			
			// manage LAST revision case
			srcRev = manageRevNb(srcId, srcRev, false);
			
			List<String> columns = new ArrayList<String>();
			columns.add(LINK_LINK_ID_COL);
			if (m_connection.isHSQL())
				columns.add(LINK_ORDER_COL);
			
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, 
					columns);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			addOrderByPart(querySB, LINK_ORDER_COL);
			String query = getEndQuery(querySB, m_connection);

			ResultSet rs = executeQuery(query);
			if (rs.next()) {
				return UUID.fromString(rs.getString(LINK_LINK_ID_COL));
			}

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
		
		return null;
	}

	public void reOrderLinks(UUID typeId, UUID srcId, int srcRev, UUID... linkIds)
			throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkRevisionParam(srcRev, false, false);
		beginInternalTransaction();
		
		if ((linkIds == null) || (linkIds.length < 2))
			throw new IllegalArgumentException("Link list must contain at least two elements.");
		
		try {
			checkTableExist(LINK_TAB);
			
			srcRev = manageRevNb(srcId, srcRev, false);
			
			// to manage ALL revision, we reorder links for each source revision 
			// from last one to first one
			int[] srcRevs = new int[] { srcRev };
			if (srcRev == ModelVersionDBService.ALL)
				srcRevs = getObjectRevNbs(srcId);
			for (int i = 0; i < srcRevs.length; i++) {
				reOrderLinksInternal(typeId, srcId, srcRevs[i], linkIds);
			}
			
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}
	
	/**
	 * Be careful, srcRev cannot be equal to ALL, LAST or ANY.
	 * 
	 * @param typeId
	 * @param srcId
	 * @param srcRev srcRev cannot be ALL, LAST or ANY
	 * @param linkIds
	 * @throws SQLException 
	 * @throws ModelVersionDBException 
	 */
	private void reOrderLinksInternal(UUID typeId, UUID srcId, int srcRev, 
			UUID... linkIds) throws SQLException, ModelVersionDBException {

		// retrieve current link order
		StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, LINK_LINK_ID_COL);
		addWherePart(querySB);
		addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
		addOrderByPart(querySB, LINK_ORDER_COL);
		String query = getEndQuery(querySB, m_connection);

		ResultSet rs = executeQuery(query);

		List<UUID> curLinkIds = new ArrayList<UUID>();
		while (rs.next()) {
			curLinkIds.add(UUID.fromString(rs.getString(1)));
		}
		
		// remove links ids which don't exist
		List<UUID> newLinkIds = new ArrayList<UUID>();
		for (int i = 0; i < linkIds.length; i++) {
			UUID linkId = linkIds[i];
			if (curLinkIds.contains(linkId))
				newLinkIds.add(linkId);
		}
		
		// compute new link order
		for (int i = 0; i < curLinkIds.size() - 1; i++) {
			UUID curLinkId = curLinkIds.get(i);
			int curIdx = newLinkIds.indexOf(curLinkId);
			if (curIdx != -1)
				continue;
			
			if (i == 0) {
				newLinkIds.add(0, curLinkId);
			} else {
				UUID prevLinkId = curLinkIds.get(i - 1);
				int prevIdx = newLinkIds.indexOf(prevLinkId);
				newLinkIds.add(prevIdx + 1, curLinkId);
			}
		}
		
		// update link order
		querySB = getBeginUpdateQuery(LINK_TAB, 
				new Assignment(LINK_ORDER_COL));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_LINK_ID_COL); // set ? as value
		addAndPart(querySB);
		addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
		query = getEndQuery(querySB, m_connection);
		PStatement prepare = createPreparedStatement(query);
		for (int i = 0; i < newLinkIds.size(); i++) {
			setValue(prepare, 1, i + 1);
			setValue(prepare, 2, newLinkIds.get(i));
			try {
				prepare.executeUpdate();
			} catch (IOException e) {
				throw new ModelVersionDBException(e);
			}
		}
		prepare.close();
	}

	public synchronized void beginTransaction() throws TransactionException {
		if (hasTransaction())
			throw new TransactionException("A transaction is already started.");
		
		m_transaction = true;
	}
	
	public void commitTransaction() throws TransactionException {
		if (!hasTransaction())
			throw new TransactionException("A transaction must be started.");
		
		// commit for all opened connections.
		boolean failed = false;
		for (ConnectionDef connection : m_connections.values()) {
			if (!connection.isClosed()) {
				if (!failed) {
					try {
						connection.commit();
						m_logger.log(Logger.DEBUG,
								"Commit transaction for connection to database "
										+ connection + " done.");
					} catch (SQLException e) {
						m_logger.log(Logger.ERROR,
								"Fail to commit transaction for connection to database "
										+ connection, e);
						failed = true;
					}
				}
				// rollback all connections if a commit fails
				if (failed) {
					try {
						connection.rollback();
						m_logger.log(Logger.DEBUG, "Rollback transaction for connection to database " + connection + " done.");
					} catch (SQLException sqlExcept) {
						m_logger.log(Logger.ERROR, "Fail to rollback transaction for connection to database "
								+ connection, sqlExcept);
					}
				}
			}
		}
		
		clearTransaction();
		
		if (failed)
			throw new TransactionException("Commit Transaction failed.");
	}
	
	public void rollbackTransaction() throws TransactionException {
		if (!hasTransaction())
			throw new TransactionException("A transaction must be started.");
		
		// rollback for all opened connections.
		boolean failed = false;
		for (ConnectionDef connection : m_connections.values()) {
			if (!connection.isClosed()) {
				try {
					connection.rollback();
				} catch (SQLException sqlExcept) {
					m_logger.log(Logger.ERROR, "Fail to rollback transaction for connection to database "
							+ connection, sqlExcept);
					failed = true;
				}
			}
		}
		
		clearTransaction();
		
		if (failed)
			throw new TransactionException("Rollback Transaction failed.");
	}

	public int createNewObjectRevision(UUID objectId, int fromRev)
			throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		checkRevisionParam(fromRev, false, false);
		beginInternalTransaction();
		
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			UUID typeId = getObjectType(objectId);
			String typeTabName = checkTypeTableExist(typeId);

			checkTableExist(OBJ_TAB);
			if (!objExists(objectId, fromRev)) {
				commitInternalTransaction();
				throw new IllegalArgumentException("Revision " + fromRev + " of object with id = "
						+ objectId + " does not exist.");
			}
			
			// manage LAST revision case
			fromRev = manageRevNb(objectId, fromRev, false);
			
			// compute new revision number
			int lastRev = getLastObjectRevNb(objectId);
			int newRev = lastRev + 1;
			
			// clone attribute values
			Map<String, String> attrColMap = new HashMap<String, String>();
			Map<String, String> attrTypeMap = new HashMap<String, String>();
			getAttributes(typeId, attrColMap, attrTypeMap);
			
			List<String> columnsList = new ArrayList<String>(); // <col value | column name>
			columnsList.add(PERTYPE_OBJ_ID_COL);
			columnsList.add(PERTYPE_REV_COL);
			for (String attName : attrColMap.keySet()) {
				columnsList.add(attrColMap.get(attName));
			}
			
			StringBuffer querySB = getBeginInsertQuery(typeTabName);
			addColumnsToInsertQuery(querySB, columnsList);
			replace(PERTYPE_REV_COL, m_connection.getTypeUtil().getSQLValue(newRev), columnsList);
			String[] columns = columnsList.toArray(new String[columnsList.size()]);
			querySB.append(getBeginSelectQuery(false, typeTabName, columns).toString());
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			addAndPart(querySB);
			addEqualPart(querySB, PERTYPE_REV_COL, fromRev);
			executeUpdate(querySB.toString(), m_connection);
			
			/*
			 * clone links
			 */
			
			// we clone each row with same source revision 
			// and set their link revision number to the sum of previous one 
			// and max link revision number  
			// very complex query but necessary for better perf and atomicity
			checkTableExist(LINK_TAB);
			
			querySB = getBeginInsertQuery(LINK_TAB);
			columnsList = getLinkTableColumns();
			addColumnsToInsertQuery(querySB, columnsList);
			replace(LINK_LINK_ID_COL, "L." + LINK_LINK_ID_COL, columnsList);
			replace(LINK_REV_COL, LINK_REV_COL + " + lastRev", columnsList);
			replace(LINK_SRC_REV_COL, m_connection.getTypeUtil().getSQLValue(newRev), columnsList);
			columns = columnsList.toArray(new String[columnsList.size()]);
			querySB.append(getBeginSelectQuery(false, LINK_TAB + " L", columns));
			querySB.append(", (");
			querySB.append(getBeginSelectQuery(false, LINK_TAB, 
					LINK_LINK_ID_COL, maxFunction(LINK_REV_COL) + " lastRev"));
			addGroupByPart(querySB, LINK_LINK_ID_COL);
			querySB.append(") L_LAST_REV ");
			addWherePart(querySB);
			querySB.append("L_LAST_REV." + LINK_LINK_ID_COL + " = L." + LINK_LINK_ID_COL);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, objectId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_REV_COL, fromRev);
			executeUpdate(querySB.toString(), m_connection);
			
			// retrieve all link types to clone
			Set<UUID> linkTypeIds = getOutgoingLinkTypes(objectId, fromRev);
			
			//clone link attributes
			for (UUID linkTypeId : linkTypeIds) {
				cloneLinkRevisions(objectId, fromRev, newRev, linkTypeId);
			}
			
			commitInternalTransaction();
			
			return newRev;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void cloneLinkRevisions(UUID objectId, int fromRev, int newRev,
			UUID linkTypeId) throws ModelVersionDBException, SQLException {
		List<String> columnsList = getLinkTableColumns();

		String typeTableName = getTypeTabName(linkTypeId);
		
		// get column names (including attribute names) to set
		Map<String, String> attrColMap = new HashMap<String, String>();
		Map<String, String> attrTypeMap = new HashMap<String, String>();
		getAttributes(linkTypeId, attrColMap, attrTypeMap);
		
		columnsList = new ArrayList<String>(); 
		columnsList.add(PERTYPE_OBJ_ID_COL);
		columnsList.add(PERTYPE_REV_COL);
		for (String attrName : attrColMap.keySet()) {
			columnsList.add(attrColMap.get(attrName));
		}
		
		// execute update : clone selected row with rev changed to a new revision
		StringBuffer querySB = getBeginInsertQuery(typeTableName);
		addColumnsToInsertQuery(querySB, columnsList);
		replace(PERTYPE_REV_COL, "L_SRC_REV." + LINK_REV_COL + " + lastRev", columnsList);
		String[] columns = columnsList.toArray(new String[columnsList.size()]);
		querySB.append(getBeginSelectQuery(false, typeTableName, columns)
				.toString());
		
		querySB.append(", (");
		querySB.append(getBeginSelectQuery(false, LINK_TAB + " L1",
				"L1." + LINK_LINK_ID_COL,
				maxFunction("L1." + LINK_REV_COL) + " lastRev").toString());
		addWherePart(querySB);
		querySB.append(" EXISTS (");
		querySB.append(getBeginSelectQuery(false, LINK_TAB + " L2",
				"*").toString());
		addWherePart(querySB);
		querySB.append("L2." + LINK_LINK_ID_COL + " = L1." + LINK_LINK_ID_COL);
		addAndPart(querySB);
		querySB.append("L2." + LINK_REV_COL + " <> L1." + LINK_REV_COL);
		addAndPart(querySB);
		querySB.append("L2." + LINK_TYPE_ID_COL + " = L1." + LINK_TYPE_ID_COL);
		addAndPart(querySB);
		querySB.append("L2." + LINK_SRC_ID_COL + " = L1." + LINK_SRC_ID_COL);
		addAndPart(querySB);
		querySB.append("L2." + LINK_SRC_REV_COL + " = L1." + LINK_SRC_REV_COL + " + 1");
		addAndPart(querySB);
		querySB.append("L2." + LINK_DEST_ID_COL + " = L1." + LINK_DEST_ID_COL);
		addAndPart(querySB);
		querySB.append("L2." + LINK_DEST_REV_COL + " = L1." + LINK_DEST_REV_COL);
		querySB.append(" )");
		addGroupByPart(querySB, LINK_LINK_ID_COL);
		querySB.append(") L_LAST_REV ");
		
		querySB.append(", (");
		querySB.append(getBeginSelectQuery(false, LINK_TAB,
				LINK_LINK_ID_COL,
				LINK_SRC_ID_COL, 
				LINK_SRC_REV_COL,
				LINK_REV_COL).toString());
		querySB.append(") L_SRC_REV ");
		
		addWherePart(querySB);
		querySB.append("L_LAST_REV." + LINK_LINK_ID_COL + " = " + PERTYPE_OBJ_ID_COL);
		addAndPart(querySB);
		querySB.append("L_LAST_REV." + LINK_LINK_ID_COL + " = L_SRC_REV." + LINK_LINK_ID_COL);
		addAndPart(querySB);
		querySB.append(typeTableName + "." + PERTYPE_REV_COL + " = L_SRC_REV." + LINK_REV_COL);
		addAndPart(querySB);
		addEqualPart(querySB, "L_SRC_REV." + LINK_SRC_ID_COL, objectId);
		addAndPart(querySB);
		addEqualPart(querySB, "L_SRC_REV." + LINK_SRC_REV_COL, fromRev);
		executeUpdate(querySB.toString(), m_connection);
	}

	private List<String> getLinkTableColumns() {
		List<String> columnsList = new ArrayList<String>(); // <col value | column name>
		columnsList.add(LINK_LINK_ID_COL);
		columnsList.add(LINK_TYPE_ID_COL);
		columnsList.add(LINK_REV_COL);
		columnsList.add(LINK_SRC_ID_COL);
		columnsList.add(LINK_SRC_REV_COL);
		columnsList.add(LINK_DEST_ID_COL);
		columnsList.add(LINK_DEST_REV_COL);
		columnsList.add(LINK_ORDER_COL);
		return columnsList;
	}

	private static void addColumnsToInsertQuery(StringBuffer queryBeginSB, 
			List<String> columns) {
		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			queryBeginSB.append(column);
			if (i < columns.size() - 1)
				queryBeginSB.append(", ");
		}
		queryBeginSB.append(") ");
	}
	
	/**
	 * Replace all occurency of specified column.
	 * 
	 * @param columnToSet
	 * @param newSQLVal
	 * @param columns
	 */
	private void replace(String columnToSet, String newSQLVal,
			List<String> columns) {
		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			
			if (columnToSet.equals(column)) {
				columns.set(i, newSQLVal);
			}
		}
	}

	public void deleteLink(UUID typeId, UUID srcId, int srcRev, UUID destId,
			int destRev) throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkRevisionParam(srcRev, true, false);
		checkDestIdParam(destId);
		checkRevisionParam(destRev, true, false);
		
		beginInternalTransaction();
		
		try {
			checkTableExist(LINK_TAB);

			String typeTableName = checkTypeTableExist(typeId);
			
			// delete link states
			StringBuffer querySB = getBeginDeleteQuery(typeTableName + " T");
			addWherePart(querySB);
			querySB.append("EXISTS (");
			querySB.append(getBeginSelectQuery(false, LINK_TAB + " L", 
					LINK_LINK_ID_COL, LINK_REV_COL).toString());
			addWherePart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			if (srcRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			}
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			if (destRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
			}
			addAndPart(querySB);
			addEqualPart(querySB, "L." + LINK_LINK_ID_COL, "T." + PERTYPE_OBJ_ID_COL);
			addAndPart(querySB);
			addEqualPart(querySB, "L." + LINK_REV_COL, "T." + PERTYPE_REV_COL);
			querySB.append(")");
			String query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
			
			// delete links in links table
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			if (srcRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_SRC_REV_COL, srcRev);
			}
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			if (destRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, LINK_DEST_REV_COL, destRev);
			}
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void deleteLink(UUID typeId, UUID srcId, int srcRev, UUID destId)
			throws ModelVersionDBException {
		deleteLink(typeId, srcId, srcRev, destId, ModelVersionDBService.ALL);
	}

	public void deleteIncomingLinks(UUID typeId, UUID destId, int destRev)
			throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkDestIdParam(destId);
		
		deleteLinksFromObjInternal(typeId, destId, destRev, false);
	}

	private void deleteLinksFromObjInternal(UUID typeId, UUID objId,
			int objRev, boolean getOutgoings) throws ModelVersionDBException {
		checkRevisionParam(objRev, true, false);
		
		String objIdCol = LINK_DEST_ID_COL;
		String objRevCol = LINK_DEST_REV_COL;
		if (getOutgoings) {
			objIdCol = LINK_SRC_ID_COL;
			objRevCol = LINK_SRC_REV_COL;
		}
		
		try {
			beginInternalTransaction();
			
			// delete link states
			// delete from <typeTable> T where exists (
			String typeTabName = null;
			try {
				typeTabName = getTypeTabName(typeId);
			} catch (IllegalArgumentException e) {
				typeTabName = null;
			}
			if (!tableExist(LINK_TAB) || (typeTabName == null) || !tableExist(typeTabName)) {
				commitInternalTransaction();
				return;
			}
			
			StringBuffer querySB = getBeginDeleteQuery(typeTabName);
			addWherePart(querySB);
			querySB.append("EXISTS (");
			
			// select <typeTabName>.OBJ_ID, <typeTabName>.REVISION from LINKS L
			// where <typeTabName>.OBJ_ID = L.LINK_ID
			// and <typeTabName>.REVISION = L.REVISION
			// and L.<object id column> = <destId>
			// and L.<object rev column> = <destRev>
			querySB.append(getBeginSelectQuery(false, LINK_TAB + " L", 
					typeTabName + "." + PERTYPE_OBJ_ID_COL, 
					typeTabName + "." + PERTYPE_REV_COL).toString());
			addWherePart(querySB);
			querySB.append(typeTabName + "." + PERTYPE_OBJ_ID_COL + " = L." + LINK_LINK_ID_COL);
			addAndPart(querySB);
			querySB.append(typeTabName + "." + PERTYPE_REV_COL + " = L." + LINK_REV_COL);
			addAndPart(querySB);
			addEqualPart(querySB, "L." + objIdCol, objId);
			if (objRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, "L." + objRevCol, objRev);
			}
			
			// );
			querySB.append(")");
			String query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
			
			// delete links in LINKS table
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, objIdCol, objId);
			if (objRev != ModelVersionDBService.ALL) {
				addAndPart(querySB);
				addEqualPart(querySB, objRevCol, objRev);
			}
			query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
			
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}
	
	public void deleteOutgoingLinks(UUID typeId, UUID srcId, int srcRev)
			throws ModelVersionDBException {
		checkConnection();
		
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		
		deleteLinksFromObjInternal(typeId, srcId, srcRev, true);
	}

	public void changeLinkDestRev(UUID linkId, int linkRev, int destRev)
			throws ModelVersionDBException {
		checkConnection();
		
		checkLinkIdParam(linkId);
		checkRevisionParam(linkRev, false, false);
		checkRevisionParam(destRev, false, false);
		
		try {
			if (!linkExists(linkId, linkRev))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			beginInternalTransaction();
			
			linkRev = manageRevNb(linkId, linkRev, true);
			UUID destId = getLinkDest(linkId);
			destRev = manageRevNb(destId, destRev, false);
			
			// effective destination revision update
			StringBuffer querySB = getBeginUpdateQuery(LINK_TAB, 
					new Assignment(LINK_DEST_REV_COL, destRev));
			addWherePart(querySB);
			addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_REV_COL, linkRev);
			String query = getEndQuery(querySB, m_connection);
			executeUpdate(query, m_connection);
			
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public boolean isType(UUID objectId) throws ModelVersionDBException {
		checkConnection();
		
		checkObjectIdParam(objectId);
		
		try {
			StringBuffer querySB = getBeginSelectQuery(true, OBJ_TAB, OBJ_IS_TYPE_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB, m_connection);
 
			ResultSet rs = executeQuery(query, false);

			if (!rs.next())
				throw new IllegalArgumentException("Object " + objectId + " does not exist.");

			return getBoolean(rs, OBJ_IS_TYPE_COL);
			
		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				throw new IllegalArgumentException("Object " + objectId + " does not exist.");
			
			throw new ModelVersionDBException(e);
		}
	}

	public boolean isObjectAttVersionSpecific(UUID typeId, String attrName)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkAttrNameParam(attrName);
		
		beginInternalTransaction();
		
		try {
			// move checking to initialization and start method
			checkTableExist(TYPE_TAB);
			
			Attribute attrJO;
			boolean isVersionSpecific = false;
			try {
				attrJO = getAttribute(typeId, attrName, true);
				isVersionSpecific = (attrJO.isVersionSpecific());
				
			} catch (IllegalArgumentException e) {
				// This attribute doesn't already exist
				
				// add new attribute definition
				int lastIdx = getAttrLastIdx(typeId);
				lastIdx++;
				String newColName = getAttrColName(lastIdx);
				String attrType = m_connection.getTypeUtil().getSQLType(null); // to set type to UNDEFINED 
				
				isVersionSpecific = DEFAULT_VERSION_SPECIFIC_FLAG;
				String query = getInsertQuery(ATTR_TAB, 
						new Assignment(ATTR_TYPE_ID_COL, typeId), 
						new Assignment(ATTR_ATTR_NAME_COL, attrName),
						new Assignment(ATTR_ATTR_TYPE_COL, attrType), 
						new Assignment(ATTR_ATTR_TAB_NAME_COL, newColName),
						new Assignment(ATTR_VERSION_SPEC_COL, isVersionSpecific));
				executeUpdate(query, m_connection);
				
				// add new column in type table
				String typeTableName = checkTypeTableExist(typeId);
				if (!tableExist(typeTableName)) {
					Map<String, String> attrTotalMap = new HashMap<String, String>();
					attrTotalMap.put(newColName, attrType);
					createTypeTable(typeTableName, attrTotalMap);
				} else {
					query = getAddColumnQuery(typeTableName, newColName, attrType);
					executeUpdate(query, m_connection);
				}
			}
			
			commitInternalTransaction();
			
			return isVersionSpecific;
		    
		} catch (Exception e) {
			throw new ModelVersionDBException(e);
		}
	}

	public void setLinkDestVersionSpecific(UUID typeId,
			boolean isVersionSpecific) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		
		beginInternalTransaction();
		
		boolean isOldVerSpec = isLinkDestVersionSpecific(typeId);
		try {
			setLinkVersionSpecificInternal(typeId, isVersionSpecific, TYPE_LINK_DEST_VERSION_SPEC_COL);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
				
		// add links if necessary
		if (!isOldVerSpec && isVersionSpecific) {
			//TODO
		}
		
		commitInternalTransaction();
	}

	public boolean isLinkDestVersionSpecific(UUID typeId)
			throws ModelVersionDBException {
		return isLinkVSInternal(typeId, TYPE_LINK_DEST_VERSION_SPEC_COL, DEFAULT_DEST_VERSION_SPECIFIC_FLAG);
	}

	private boolean isLinkVSInternal(UUID typeId, String verSpecCol, 
			boolean defaultVal) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		
		StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB, verSpecCol);
		addWherePart(querySB);
		addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
		String query = getEndQuery(querySB, m_connection);

		try {
			ResultSet rs = executeQuery(query);

			if (!rs.next()) {
				return defaultVal;
			}

			return getBoolean(rs, verSpecCol);
		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return defaultVal;
			
			throw new ModelVersionDBException(e);
		}
	}

	public boolean isLinkSrcVersionSpecific(UUID typeId)
			throws ModelVersionDBException {
		return isLinkVSInternal(typeId, TYPE_LINK_SRC_VERSION_SPEC_COL, DEFAULT_SRC_VERSION_SPECIFIC_FLAG);
	}

	public boolean hasTransaction() {
		return m_transaction;
	}

}
