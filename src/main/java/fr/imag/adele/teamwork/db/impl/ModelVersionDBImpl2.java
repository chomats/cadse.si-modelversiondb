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


import java.util.UUID;
import fr.imag.adele.teamwork.db.DBIteratorID;
import static fr.imag.adele.teamwork.db.impl.SQLConstants.*;
import static fr.imag.adele.teamwork.db.impl.TableConstants2.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import bak.pcj.IntIterator;
import bak.pcj.list.IntArrayList;
import bak.pcj.list.IntList;
import bak.pcj.map.IntKeyOpenHashMap;
import bak.pcj.set.IntBitSet;
import fr.imag.adele.cadse.core.CadseException;
import java.util.UUID;

import org.apache.felix.ipojo.util.Logger;
import org.osgi.framework.BundleContext;

import fr.imag.adele.cadse.core.ItemType;
import fr.imag.adele.cadse.core.LinkType;
import fr.imag.adele.cadse.core.LogicalWorkspace;
import fr.imag.adele.cadse.core.attribute.IAttributeType;
import fr.imag.adele.cadse.core.transaction.delta.ItemDelta;
import fr.imag.adele.cadse.core.transaction.LogicalWorkspaceTransaction;
import fr.imag.adele.cadse.util.ArraysUtil;
import fr.imag.adele.cadse.util.NLS;
import fr.imag.adele.teamwork.db.ID3;
import fr.imag.adele.teamwork.db.LinkInfo;
import fr.imag.adele.teamwork.db.LinkInfoPlus;
import fr.imag.adele.teamwork.db.ModelVersionDBException;
import fr.imag.adele.teamwork.db.ModelVersionDBService;
import fr.imag.adele.teamwork.db.ModelVersionDBService2;
import fr.imag.adele.teamwork.db.TransactionException;

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
public class ModelVersionDBImpl2 implements ModelVersionDBService2 {

    
	private static final String CADSEDB_NAME = "cadsedb";

	private static final String SA = "SA";

	private static final String	LINK_TYPE_SQL_TYPE	= "{#LinkType}";

	public static final int[] EMPTY_IDS = new int[0];
	public static final DBIteratorID EMPTY_IDS_ITER = new DBIteratorID() {

		public ID3 next() {
			return null;
		}

		public boolean hasNext() {
			return false;
		}
	};

    @Override
    public <T> DBIteratorID<T> getOutgoingLinksAggregation(Class<T> kind, int objId) throws ModelVersionDBException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


	public static class TableInfo {
		private static final TableAttribute2[]	EMPTY	= new TableAttribute2[0];

		TableInfo(int typeId, int... tableIDCol) {
			this.typeId = typeId;
			_attributes = null;
			for (int n : tableIDCol) {
				String name = null;
				if (n<-32)
					name = ATTRIBUTES_COLS_NAME[decal_attr+n];
				new TableAttribute2(name, n, SQLTypes.INT_SQL_TYPE_DEF, this, true);
			}
		}

		TableInfo(int typeId) {
			this.typeId = typeId;
			_attributes = null;
		}

		int typeId = -1;
		boolean created = false;

		private TableAttribute2[] _attributes;

		void add(TableAttribute2 at) {
			_attributes = ArraysUtil.add(TableAttribute2.class, _attributes, at);
		}

		public TableAttribute2 addAttribute(int attrId, String sqlType) {
			return new TableAttribute2(null, attrId, sqlType, this, false);
		}

		public TableAttribute2[] getAttributes() {
			if (_attributes == null)
				return EMPTY;
			return _attributes;
		}

		public String getTableName() {
			return getTypeTabName(typeId);
		}
	}

	public static class TableAttribute2 {
		public TableAttribute2(String name, int attr, String attrType, TableInfo parent, boolean id) {
			this.attributeId = attr;
			this.sqlType = attrType;
			this.parent = parent;
			this.id = id;
			this.name = name;
			this.parent.add(this);
		}
		String name;
		TableInfo parent ;
		int attributeId;
		String sqlType;
		boolean id;
		public int	typeOfAttributeId;

		public String getColumn() {
			return ATTR_COL_PREFIX+attributeId;
		}

		public int getTypeId() {
			return parent.typeId;
		}

		public String getType() {
			return sqlType;
		}
	}


	public class Metadata {
		IntKeyOpenHashMap _tables = new IntKeyOpenHashMap();
		IntKeyOpenHashMap _attributes = new IntKeyOpenHashMap();

		public Metadata() {
			initTable();
		}

		private TableInfo addTableInfo(int typeId, int... tableIDCol) {
			TableInfo ti = new TableInfo(typeId, tableIDCol);
			_tables.put(ti.typeId, ti);
			return ti;
		}

		void initTable() {
			addTableInfo(ID_OBJ_TAB, ID_OBJ_OBJ_ID_COL, ID_OBJ_CADSE_COL, ID_OBJ_PARENT_COL);
			addTableInfo(ID_OBJ_TYPE_TAB, ID_OBJ_TYPE_OBJ_ID_COL, ID_OBJ_TYPE_TYPE_ID_COL);
			addTableInfo(ID_TYPE_SUPER_TAB, ID_TYPE_SUPER_SUB_TYPE_ID_COL, ID_TYPE_SUPER_SUPER_TYPE_ID_COL);
			addTableInfo(ID_TYPE_EXT_TAB, ID_TYPE_EXT_TYPE_ID_COL, ID_TYPE_EXT_EXT_TYPE_ID_COL);
			addTableInfo(ID_TYPE_TAB, ID_TYPE_TYPE_ID_COL);
			addTableInfo(ID_ATTR_TAB, ID_ATTR_TYPE_ID_COL, ID_ATTR_ATTR_ID_COL);
			addTableInfo(ID_LINK_TAB, ID_LINK_LINK_ID_COL, ID_LINK_TYPE_ID_COL, ID_LINK_SRC_ID_COL, ID_LINK_DEST_ID_COL);
			addTableInfo(ID_UUID_TAB, ID_UUID_TAB_ID_COL);
			addTableInfo(ID_ALL_OBJ_TYPE_TAB, ID_ALL_OBJ_TYPE_OBJ_ID_COL, ID_ALL_OBJ_TYPE_OBJ_TYPE_ID_COL);
			addTableInfo(ID_ALL_EXT_TAB, ID_ALL_EXT_TYPE_ID_COL, ID_ALL_EXT_EXT_TYPE_ID_COL);
			addTableInfo(ID_ALL_TYPE_SUPER_TAB, ID_ALL_TYPE_SUPER_SUB_TYPE_ID_COL, ID_ALL_TYPE_SUPER_SUPER_TYPE_ID_COL);
		}

		public TableAttribute2 getAttribute(int attrId) {
			return (TableAttribute2) _attributes.get(attrId);
		}

		public IntKeyOpenHashMap getAttributesDefinition(int objectId) throws SQLException, IOException {
			IntKeyOpenHashMap attrsDefinition;
			String query =
				"select "+ATTR_ATTR_ID_COL+
				", "+ATTR_TYPE_ID_COL+", "+ATTR_SQL_TYPE_COL
				+" from "+ALL_OBJ_TYPE_TAB+" aot, "+ATTR_TAB+" a "+
			"where aot."+ALL_OBJ_TYPE_OBJ_TYPE_ID_COL+
			" = a."+ATTR_TYPE_ID_COL+" and aot.OBJ_ID = ?";

			PStatement ps = createPreparedStatement(query);
			ps.getStatement().setInt(1, objectId);
			ResultSet rs = ps.executeQuery();
			attrsDefinition = new IntKeyOpenHashMap();
			while (rs.next()) {
				int attrId = rs.getInt(1);
				TableAttribute2 tattr = getAttribute(attrId);
				if (tattr != null)
					continue;

				int typeId = rs.getInt(2);
				TableInfo ta = (TableInfo) attrsDefinition.get(typeId);
				if (ta == null) {
					ta = getOrCreateTableInfo(typeId);
					attrsDefinition.put(typeId, ta);
				}

				String sqlType = rs.getString(3);
				createAttribute(ta, attrId, sqlType, -1, null);
			}
			ps.close();
			return attrsDefinition;
		}

		public TableInfo getTableInfo(int typeId, boolean load) throws SQLException, ModelVersionDBException {
			// get attributes in DB
			TableInfo ti = (TableInfo) _tables.get(typeId);
			if (ti != null)
				return ti;

			TableInfo ret = new TableInfo(typeId);
			_tables.put(typeId, ti);

			String query = getQuery(query(SELECT, ID_ATTR_TYPE_ID_COL, ID_ATTR_SQL_TYPE_COL,
					ID_OBJ_TYPE_ID_COL, ID_OBJ_NAME_COL,
					FROM, ID_ATTR_TAB, ID_OBJ_TAB,
					WHERE,
						ID_ATTR_TYPE_ID_COL, EQUAL, P0 ,
						AND, ID_OBJ_OBJ_ID_COL, EQUAL, ID_ATTR_ATTR_ID_COL), args(typeId));

			ResultSet rs = null;

			try {
				rs = executeQuery(query);
				while (rs.next()) {
					createAttribute(ret, rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getString(4));
				}
				return ret;
			} finally {
				close(rs);
			}
		}


		private void createAttribute(TableInfo ta, int attrId, String sqlType, int attrTypeId, String name) {
			TableAttribute2 tattr;
			tattr = ta.addAttribute(attrId, sqlType);
			tattr.name = name;
			tattr.typeOfAttributeId = attrTypeId;
			_attributes.put(attrId, tattr);
		}

		private TableInfo getOrCreateTableInfo(int typeId) {
			TableInfo ta;
			ta = (TableInfo) _tables.get(typeId);
			if (ta == null) {
				ta = new TableInfo(typeId);
				ta.typeId = typeId;
				_tables.put(typeId, ta);
				try {
					tableExist(typeId);
				} catch (ModelVersionDBException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return ta;
		}

		public void removeTable(int tableId) {
			TableInfo ti = (TableInfo) _tables.remove(tableId);
			if (ti != null)
				for (TableAttribute2 ta : ti.getAttributes()) {
					_attributes.remove(ta.attributeId);
				}
		}

		public TableAttribute2 getAttribute(int attr, boolean throwExceptionIfNoAttrDef)
		throws ModelVersionDBException, SQLException {
				// get attributes in DB
				TableAttribute2 ret = (TableAttribute2) _attributes.get(attr);
				if (ret != null)
					return ret;
				checkTableExist(ID_ATTR_TAB);
				String query =
					getQuery(query(SELECT, ID_ATTR_TYPE_ID_COL, ID_ATTR_SQL_TYPE_COL,
							ID_OBJ_TYPE_ID_COL, ID_OBJ_NAME_COL,
							FROM, ID_ATTR_TAB, ID_OBJ_TAB,
							WHERE,
								ID_ATTR_ATTR_ID_COL, EQUAL, P0 ,
								AND, ID_OBJ_OBJ_ID_COL, EQUAL, P0), args(attr));

				String attrType;
				int typeId = -1;
				int attrTypeId;
				String attrName;
				ResultSet rs = null;
				try {
					rs = executeQuery(query);

					if (!rs.next()) {
						if (throwExceptionIfNoAttrDef) {
							throw new IllegalArgumentException("Cannot find attribute " + attr);
						} else {
							return null;
						}
					}
					typeId = rs.getInt(1);
					attrType = rs.getString(2);
					attrTypeId = rs.getInt(3);
					attrName = rs.getString(4);

				} finally {
					close(rs);
				}

				TableInfo ti = (TableInfo) _tables.get(typeId);
				if (ti == null) {
					ti = addTableInfo(typeId);
				}
				TableAttribute2 ta = new TableAttribute2(null, attr, attrType, ti, false);
				ta.name = attrName;
				ta.typeOfAttributeId = attrTypeId;
				_attributes.put(attr, ta);
				return ta;
			}
	}




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



	// Logger
	Logger m_logger;

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

	private Metadata	_metadata;

	private BundleContext _bc;


	public ModelVersionDBImpl2(BundleContext bc) {
		m_logger = new Logger(bc, "Registry logger " + bc.getBundle().getBundleId(), Logger.WARNING);
		m_enable = true;
		_bc = bc;
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
			if (this.getClass().getClassLoader() != null)
				this.getClass().getClassLoader().loadClass(driverName).newInstance();
			else {
				Class.forName(driverName).newInstance();
			}
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
		//
		try {
			setConnectionURL(ModelVersionDBService.HSQL_IN_FILE, null, 0, _bc.getDataFile(CADSEDB_NAME).getAbsolutePath(),
					SA, "");
		} catch (TransactionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

		executeUpdate(getEndQuery(querySB));
	}

	private void executeResetSequence(String seqName, int newStart) throws SQLException {
		if (!m_connection.isOracle()) {
			executeUpdate(getResetSequenceQuery(seqName, newStart));
			return;
		}

		// Oracle cannot restart a sequence
		executeUpdate(getDropSequenceQuery(m_connection, seqName));
		executeCreateSequence(seqName, newStart);
	}

	public void stop() {
		m_started = false;
		
		m_connection = null;
		clearTransaction();
		try {
			Connection c = DriverManager.getConnection(
			        "jdbc:hsqldb:file:"+
			        _bc.getDataFile(CADSEDB_NAME).getAbsolutePath()+";shutdown=true", SA, "");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// close opened connection in clear transaction

		m_connections.clear();
	}

	private synchronized void closeConnection(ConnectionDef connection) {
		try {
			if (!connection.isClosed()) {
				rollbackAllInternalTransactions(connection);

				// remove sequences
				if (!connection.isMySQL()) {
					executeUpdate(connection, getDropSequenceQuery(connection, connection.getLastSeqName()));
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

	private String getDropSequenceQuery(ConnectionDef connection, String seqName) {
		StringBuffer querySB = new StringBuffer();
		querySB.append("DROP SEQUENCE ");
		querySB.append(seqName);
		if (connection.isHSQL()) {
			querySB.append(" IF EXISTS");
		}

		return getEndQuery(connection, querySB);
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
			///m_logger.log(Logger.DEBUG, "[ERROR IN DATABASE ACCESS] A SQLException occurs in executeQuery("
				///			+ sql + ") : " + e.getMessage(), e);
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

	public ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql, true);
	}

	public ResultSet executeQuery(String sql, boolean printError) throws SQLException {
		m_logger.log(Logger.DEBUG, "Execute sql query : " + sql);
		m_logger.log(Logger.INFO, "Open statement " + sql);
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
	private int executeUpdate(String sql) throws SQLException {
		return executeUpdate(m_connection, sql);
	}
	
	private int executeUpdate(ConnectionDef connection, String sql) throws SQLException {
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
			close(stat);
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
	public synchronized void saveObject(int objectId,
			int state, int[] localTypeIds, int localCadseId, String qualifiedName,
			String name, Map<Integer, Object> stateMap) throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(localTypeIds, objectId);
		checkObjectIdParam(objectId);
		beginInternalTransaction();

		try {
			// move checking to initialization and start method
			checkTableExist(ID_TYPE_TAB);
			checkTableExist(ID_OBJ_TAB);
			checkTableExist(ID_OBJ_TYPE_TAB);
			checkTableExist(ID_ALL_EXT_TAB);
			checkTableExist(ID_ALL_TYPE_SUPER_TAB);
			checkTableExist(ID_ALL_OBJ_TYPE_TAB);


			if (objExists(objectId)) {

				//type is not final.
				StringBuffer querySB =  getBeginUpdateQuery(OBJ_TAB,
						new Assignment(OBJ_CADSE_COL),
						new Assignment(OBJ_NAME_COL),
						new Assignment(OBJ_QNAME_COL),
						new Assignment(OBJ_STATE_COL),
						new Assignment(OBJ_TYPE_ID_COL));

				addWherePart(querySB);
				addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
				String query = getEndQuery(querySB);

				PStatement ps = createPreparedStatement(query);
				ps.getStatement().setInt(1, localCadseId);
				ps.getStatement().setString(2, name);
				ps.getStatement().setString(3, qualifiedName);
				ps.getStatement().setInt(4, state);
				ps.getStatement().setInt(5, localTypeIds.length==0 ?NULL_ID :localTypeIds[0]);
				ps.executeUpdate();
				ps.close();
				setIds(ID_OBJ_TYPE_TAB,
						OBJ_TYPE_OBJ_ID_COL, OBJ_TYPE_TYPE_ID_COL, objectId, true, false, localTypeIds);

			} else {
				String query = getInsertQuery(OBJ_TAB,
						new Assignment(OBJ_OBJ_ID_COL),
						new Assignment(OBJ_CADSE_COL),
						new Assignment(OBJ_NAME_COL),
						new Assignment(OBJ_QNAME_COL),
						new Assignment(OBJ_STATE_COL),
						new Assignment(OBJ_TYPE_ID_COL));

				PStatement ps = createPreparedStatement(query);
				ps.getStatement().setInt(1, objectId);
				ps.getStatement().setInt(2, localCadseId);
				ps.getStatement().setString(3, name);
				ps.getStatement().setString(4, qualifiedName);
				ps.getStatement().setInt(5, state);
				ps.getStatement().setInt(6, localTypeIds.length==0 ?NULL_ID :localTypeIds[0]);
				ps.executeUpdate();
				ps.close();

				query = getInsertQuery(OBJ_TYPE_TAB,
						new Assignment(OBJ_TYPE_OBJ_ID_COL),
						new Assignment(OBJ_TYPE_TYPE_ID_COL),
						new Assignment(ORDER_COL));

				for (int i = 0; i < localTypeIds.length; i++) {
					m_logger.log(Logger.INFO, "insert into "+OBJ_TYPE_TAB+"<"+objectId+", "+localTypeIds[i]+" "+i);
					ps = createPreparedStatement(query);
					ps.getStatement().setInt(1, objectId);
					ps.getStatement().setInt(2, localTypeIds[i]);
					ps.getStatement().setInt(3, i);
					ps.executeUpdate();
					ps.close();
				}
			}

			checkTableExist(ID_ATTR_TAB);
			try {
				storeState(objectId, stateMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void finishImport(int objectId) throws ModelVersionDBException {
		if (isInstanceof(objectId, CadseGPackage.LocalId.ITEM_TYPE)) {
			int[] superTypeId = getOutgoingLinkDests(CadseGPackage.LocalId.ITEM_TYPE_lt_SUPER_TYPE, objectId);
			saveObjectType(objectId, superTypeId, EMPTY_IDS, EMPTY_IDS);
		}
		if (isInstanceof(objectId, CadseGPackage.LocalId.LINK_TYPE)) {
			int src = getOutgoingLinkDest(CadseGPackage.LocalId.LINK_TYPE_lt_SOURCE, objectId);
		}
	}

	private boolean existNupplet(String tabName, String col1, int id1,
			String col2, int id2) throws ModelVersionDBException  {
		ResultSet rs = null;
                try {
			StringBuffer querySB = getBeginSelectQuery(true, tabName);
			addWherePart(querySB);
			addEqualPart(querySB, col1, id1);
			addAndPart(querySB);
			addEqualPart(querySB, col2, id2);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);
			return (rs.next());

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return false;
			throw new ModelVersionDBException(e);
		} finally {
                    close(rs);
                }
	}

	public void storeOrderedList(int tabName, String Col1, String Col2, int v1, int[] v2, boolean insert) throws SQLException, IOException, ModelVersionDBException {
		checkTableExist(tabName);
		String query;
		if (insert) {
			query = getInsertQuery(getTypeTabName(tabName), new Assignment(Col1), new Assignment(Col2), new Assignment(ORDER_COL));
			if (v2 != null)
				for (int i = 0; i < v2.length; i++) {
					PStatement ps = createPreparedStatement(query);
					ps.getStatement().setInt(1, v1);
					ps.getStatement().setInt(2, v2[i]);
					ps.getStatement().setInt(3, i);
					ps.executeUpdate();
					ps.close();
				}
		} else {

		}
	}

	public void saveObjectType(int objectId, int[] superTypeId, int[] extendedBy, int[] attributes)
	throws ModelVersionDBException {
		if (!objExists(objectId)) {
			throw new IllegalArgumentException("oject not exists.");
		}
		checkConnection();

		beginInternalTransaction();

		try {
//			if (superTypeId != null) {
////				for (int i = 0; i < superTypeId.length; i++) {
////					if (!isType(superTypeId[i]))
////						throw new IllegalArgumentException("type not exists: "+superTypeId[i]);
////				}
//			}
//			if (extendedBy != null) {
//				for (int i = 0; i < extendedBy.length; i++) {
//					if (!isType(extendedBy[i]))
//						throw new IllegalArgumentException("type not exists: "+extendedBy[i]);
//				}
//			}
//			if (attributes != null) {
//				for (int i = 0; i < attributes.length; i++) {
//					if (!isAttributeDefinition(attributes[i]))
//						throw new IllegalArgumentException("attribute definition not exists: "+attributes[i]);
//				}
//			}


			if (isType(objectId)) {

			} else {
				checkTableExist(ID_TYPE_TAB);
				String query = getInsertQuery(TYPE_TAB,
						new Assignment(TYPE_TYPE_ID_COL));
				PStatement ps = createPreparedStatement(query);
				ps.getStatement().setInt(1, objectId);
				ps.executeUpdate();
				ps.close();

				checkTableExist(ID_TYPE_SUPER_TAB);
				query = getInsertQuery(TYPE_SUPER_TAB,
						new Assignment(TYPE_SUPER_SUB_TYPE_ID_COL),
						new Assignment(TYPE_SUPER_SUPER_TYPE_ID_COL),
						new Assignment(ORDER_COL));

				if (superTypeId != null)
					setSuperType(objectId, superTypeId);

				checkTableExist(ID_TYPE_EXT_TAB);
				query = getInsertQuery(TYPE_EXT_TAB,
						new Assignment(TYPE_EXT_TYPE_ID_COL),
						new Assignment(TYPE_EXT_EXT_TYPE_ID_COL),
						new Assignment(ORDER_COL));
				if (extendedBy != null)
					ps = createPreparedStatement(query);
					for (int i = 0; i < extendedBy.length; i++) {
						ps.getStatement().setInt(1, objectId);
						ps.getStatement().setInt(2, extendedBy[i]);
						ps.getStatement().setInt(3, i);
						ps.executeUpdate();
					}
					ps.close();
				getMetadata().addTableInfo(objectId, ID_PERTYPE_OBJ_ID_COL);
			}
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public int getObjectStateInt(int objectId) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, OBJ_TAB, OBJ_STATE_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			if (!rs.next())
				return -2;

			return rs.getInt(1);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return -2;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
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
			checkTableExist(ID_GEN_TAB);

			// get last used savepoint index
			StringBuffer querySB = getBeginSelectQuery(false, GEN_TAB,
					GEN_LAST_IDX_COL);
			addWherePart(querySB);
			addEqualPart(querySB, GEN_TYPE_COL, idxType);
			String query = getEndQuery(querySB);

			ResultSet rs = executeQuery(query);
			boolean hasLastSavePopintIdx = rs.next();

			if (!hasLastSavePopintIdx) {
				close(rs);
				// TODO workaround for a bug with mysql databases
				addDefaultEntries(ID_GEN_TAB);

				querySB = getBeginSelectQuery(false, GEN_TAB,
						GEN_LAST_IDX_COL);
				addWherePart(querySB);
				addEqualPart(querySB, GEN_TYPE_COL, idxType);
				query = getEndQuery(querySB);
				rs = executeQuery(query);
				boolean hasEntry = rs.next();
				if (!hasEntry)
					throw new IllegalStateException("Entry " + idxType
						+ " should exists in table " + GEN_TAB);
			}

			int new_idx = rs.getInt(1) + 1;
			close(rs);

			// store new table name index
			querySB = getBeginUpdateQuery(GEN_TAB,
					new Assignment(GEN_LAST_IDX_COL));
			addWherePart(querySB);
			addEqualPart(querySB, GEN_TYPE_COL, idxType);
			query = getEndQuery(querySB);

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
			m_logger.log(Logger.DEBUG, "Commit savepoint " + savepoint.getSavepointName());
			if (!m_connection.isOracle())
				m_connection.getConnection().releaseSavepoint(savepoint);
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
			} catch (SQLException sqlSEVERE) {
				m_logger.log(Logger.ERROR, "Internal rollback failed.", sqlSEVERE);
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

	private void storeState(int objectId,
			Map<Integer, Object> stateMap)
			throws SQLException, ModelVersionDBException, IOException {

		IntKeyOpenHashMap attrsDefinition = getMetadata().getAttributesDefinition(objectId);

		Iterator iter = attrsDefinition.values().iterator();
		while (iter.hasNext()) {
			TableInfo ti = (TableInfo) iter.next();

			if (!tableExist(ti.typeId))
				createTypeTable(ti);
			// check if
			String typeTabName = getTypeTabName(ti.typeId);
			createObjectRowIfNeed(objectId, typeTabName);
			if ((stateMap == null) || (stateMap.isEmpty())) {
				continue;
			}

			List<Assignment> assigns = new ArrayList<Assignment>();
			List<Object> values = new ArrayList<Object>();
			if (ti._attributes == null) continue;
			for (TableAttribute2 attr : ti._attributes) {
				Object v = stateMap.get(attr);
				if (v == null) continue;
				String colName = attr.getColumn();
				assigns.add(new Assignment(colName));
				values.add(v);
			}
			if (assigns.size() == 0) return;
			updateAttrValuesInternal(objectId, typeTabName, assigns, values);
		}

	}

	private void createObjectRowIfNeed(int objectId, String typeTabName)
			throws SQLException {
		StringBuffer querySB = getBeginSelectQuery(false, typeTabName, "*");
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);

		String query = getEndQuery(querySB);
		ResultSet rs = executeQuery(query);
		boolean rowExist = rs.next();
		close(rs);

		// insert or update object row
		if (!rowExist) {
			// add new row
			query = getInsertQuery(typeTabName,
					new Assignment(PERTYPE_OBJ_ID_COL, objectId));
			executeUpdate(query);
		}
	}



	private void updateAttrValuesInternal(int objectId, String typeTabName,
			List<Assignment> assigns, List<Object> values) throws SQLException,
			ModelVersionDBException {
		StringBuffer querySB;
		String query;
		querySB = getBeginUpdateQuery(typeTabName, assigns
					.toArray(new Assignment[assigns.size()]));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);

		query = getEndQuery(querySB);
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
		if (value instanceof Enum) {
			prepare.getStatement().setInt(idx, ((Enum) value).ordinal());
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


	private void migrateCol(int typeId, int attrID, String newType, String colName) throws ModelVersionDBException, SQLException {
		String query;
		// Alter column a new column for attribute values
		query = getAlterTableColumnQuery(getTypeTabName(typeId), colName, newType);
		executeUpdate(query);
	}

	private String getAddColumnQuery(String tableName, String colName,
			String attrType) {
		return getAlterTableColumnQuery(tableName, colName, attrType, true);
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

		return getEndQuery(sb);
	}

	private String getAlterTableColumnQuery(String tableName, String colName,
			String attrType) {
		// Remove type annotations
	    String attributeType = TypeUtil.removeTypeAnnotations(attrType);

		// construct query
		StringBuffer sb = new StringBuffer("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" ");
		sb.append("ALTER");
		sb.append(" COLUMN ");
		sb.append(colName);
		sb.append(" ");
		sb.append(attributeType);

		return getEndQuery(sb);
	}

	private void createTypeTable(TableInfo ti) throws SQLException {
		StringBuffer querySB = getBeginCreateTableQuery(getTypeTabName(ti.typeId));
		addCreateTableColumnPart(querySB, PERTYPE_OBJ_ID_COL, m_connection.getTypeUtil().getInteger(), true);
		for (TableAttribute2 ta : ti.getAttributes()) {
			if (ta.attributeId == ID_PERTYPE_OBJ_ID_COL) continue; //special attribute
			if (ta.getType().contains(LINK_TYPE_SQL_TYPE)) continue;
			String colName = ta.getColumn();
			String colType = TypeUtil.removeTypeAnnotations(ta.getType());
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, colName, colType, false);
		}
		String query = getEndCreateTableQuery(querySB);
		executeUpdate(query);
		ti.created = true;
	}

	/**
	 * Return name of table where objects of specified should be stored.
	 * This method doesn't create this table if it doesn't already exist.
	 *
	 * @param typeId
	 * @return
	 * @throws ModelVersionDBException
	 */
	static private String getTypeTabName(int typeId)  {
		if (typeId <-1) {
			return TABS[decal_tab+typeId];
		}
		else
			return "T_" + typeId;
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

	private boolean typeExistsInTypeTable(int typeId)
			throws ModelVersionDBException {
		try {
			StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB,
					TYPE_TYPE_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, TYPE_TYPE_ID_COL, typeId);
			String query = getEndQuery(querySB);

			ResultSet rs = executeQuery(query);
			boolean typeExist = rs.next();
			close(rs);

			return (typeExist);

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private void checkTableExist(int tableName) throws SQLException, ModelVersionDBException {
		if (!tableExist(tableName)) {

			if (tableName == ID_ALL_OBJ_TYPE_TAB) {
				checkTableExist(ID_ALL_TYPE_SUPER_TAB);
				checkTableExist(ID_OBJ_TYPE_TAB);
				checkTableExist(ID_ALL_EXT_TAB);
				String query =
					getQuery(query(CREATE, VIEW, ID_ALL_OBJ_TYPE_TAB, OPEN_PAR, ID_ALL_OBJ_TYPE_OBJ_ID_COL, ID_ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, CLOSE_PAR,
							AS,
							SELECT, ID_OBJ_TYPE_OBJ_ID_COL, ID_OBJ_TYPE_TYPE_ID_COL, FROM, ID_OBJ_TYPE_TAB,
							UNION, DISTINCT,
							SELECT,
								T1, ID_OBJ_TYPE_OBJ_ID_COL,
								T0, ID_ALL_TYPE_SUPER_SUPER_TYPE_ID_COL,
							FROM,
								ID_ALL_TYPE_SUPER_TAB, T0,
								ID_OBJ_TYPE_TAB, T1,
							WHERE,
								T0, ID_ALL_TYPE_SUPER_SUB_TYPE_ID_COL, EQUAL, T1, ID_OBJ_TYPE_TYPE_ID_COL,
							UNION, DISTINCT,
							SELECT,
								T1, ID_OBJ_TYPE_OBJ_ID_COL,
								T0, ID_ALL_EXT_EXT_TYPE_ID_COL,
							FROM,
								ID_ALL_EXT_TAB, T0,
								ID_OBJ_TYPE_TAB, T1,
							WHERE, T0, ID_ALL_EXT_TYPE_ID_COL, EQUAL, T1, ID_OBJ_TYPE_TYPE_ID_COL),args());
				executeUpdate(query);
				return;
				//select type "type" form obj_type where obj-id = ?
				//union distinct
				//select all_type.super_type "type" from all_type.sub-type = obj-type.type and obj-id = ?
			}
			StringBuffer querySB = getBeginCreateTableQuery(getTypeTabName(tableName));
			addColumnsDef(querySB, tableName);
			String query = getEndCreateTableQuery(querySB);

			executeUpdate(query);

			addDefaultEntries(tableName);
		}
	}

	private void addDefaultEntries(int tableId) throws SQLException {
		if (ID_GEN_TAB == tableId) {
			String tableName = getTypeTabName(tableId);
			String query = getInsertQuery(tableName ,
					new Assignment(	GEN_TYPE_COL, TableConstants.TYPE_TYPE_NAME, m_connection.getTypeUtil()),
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query);

			query = getInsertQuery(tableName, new Assignment(
					GEN_TYPE_COL, TableConstants.TYPE_SAVEPOINT_NAME, m_connection.getTypeUtil()),
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query);

			query = getInsertQuery(tableName, new Assignment(
					GEN_TYPE_COL, TableConstants.TYPE_CLIENT_NAME, m_connection.getTypeUtil()),
					new Assignment(GEN_LAST_IDX_COL, 0, m_connection.getTypeUtil()));
			executeUpdate(query);
		}
	}

	private void addColumnListDef(StringBuffer querySB, String col1, String col2, boolean addOrder) {
		addCreateTableColumnPart(querySB, col1, m_connection.getTypeUtil().getInteger(), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, col2, m_connection.getTypeUtil().getInteger(), false);
		if (addOrder) {
			addColDefSeparator(querySB);
			addCreateTableColumnPart(querySB, ORDER_COL, m_connection.getTypeUtil().getInteger(), false);
		}
		addMultiplePKPart(querySB, null, true, col1, col2);
		if (addOrder)
			addMultiplePKPart(querySB, null, false, col1, ORDER_COL);
	}



	private void addColumnsDef(StringBuffer querySB, int tableId) {
		switch (tableId) {
			case ID_FREE_ID_TAB:
				addCreateTableColumnPart(querySB, FREE_ID_COL, m_connection.getTypeUtil().getInteger(), true);
				return;
			case ID_ALL_OBJ_TYPE_TAB:
				//addColumnListDef(querySB, ALL_OBJ_TYPE_OBJ_ID_COL, ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, false);
				return;
			case ID_ALL_EXT_TAB:
				addColumnListDef(querySB, ALL_EXT_TYPE_ID_COL, ALL_EXT_EXT_TYPE_ID_COL, false);
				return;
			case ID_ALL_TYPE_SUPER_TAB:
				addColumnListDef(querySB, ALL_TYPE_SUPER_SUB_TYPE_ID_COL, ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, false);
				return;
			case ID_UUID_TAB:
				addCreateTableColumnPart(querySB, UUID_TAB_ID_COL, m_connection.getTypeUtil().getInteger(), true);
				addColDefSeparator(querySB);

				addCreateTableColumnPart(querySB, UUID_TAB_MSB_COL, m_connection.getTypeUtil().getLongType(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, UUID_TAB_LSB_COL, m_connection.getTypeUtil().getLongType(), false);
				addMultiplePKPart(querySB, null, false, UUID_TAB_MSB_COL, UUID_TAB_LSB_COL);
				return;
			case ID_OBJ_TAB:
				addCreateTableColumnPart(querySB, OBJ_OBJ_ID_COL, m_connection.getTypeUtil().getInteger(), true);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_NAME_COL, m_connection.getTypeUtil().getSQLText(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_QNAME_COL, m_connection.getTypeUtil().getSQLText(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_STATE_COL, m_connection.getTypeUtil().getInteger(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_CADSE_COL, m_connection.getTypeUtil().getInteger(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_TYPE_ID_COL, m_connection.getTypeUtil().getInteger(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, OBJ_PARENT_COL, m_connection.getTypeUtil().getInteger(), false);
				return;
			case ID_OBJ_TYPE_TAB:
				addColumnListDef(querySB, OBJ_TYPE_OBJ_ID_COL, OBJ_TYPE_TYPE_ID_COL, true);
				return;
			case ID_TYPE_SUPER_TAB:
				addColumnListDef(querySB, TYPE_SUPER_SUB_TYPE_ID_COL, TYPE_SUPER_SUPER_TYPE_ID_COL, true);
				return;
			case ID_TYPE_EXT_TAB:
				addColumnListDef(querySB, TYPE_EXT_TYPE_ID_COL, TYPE_EXT_EXT_TYPE_ID_COL, true);
				return;
			case ID_TYPE_TAB:
				addCreateTableColumnPart(querySB, TYPE_TYPE_ID_COL, m_connection.getTypeUtil().getInteger(), true);
				return;
			case ID_GEN_TAB:
				addCreateTableColumnPart(querySB, GEN_TYPE_COL, TypeUtil
						.getVarcharDef(GEN_TYPE_LENGTH), true);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, GEN_LAST_IDX_COL,
						m_connection.getTypeUtil().getInteger(), false);
				return;
			case ID_ATTR_TAB:
				addCreateTableColumnPart(querySB, ATTR_TYPE_ID_COL, m_connection.getTypeUtil().getInteger(), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, ATTR_ATTR_ID_COL, m_connection.getTypeUtil().getInteger(), true);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, ATTR_SQL_TYPE_COL, TypeUtil
						.getVarcharDef(35), false);
				addColDefSeparator(querySB);
				addCreateTableColumnPart(querySB, ATTR_JAVA_TYPE_COL, m_connection.getTypeUtil().getSQLText(), false);
				return;
			case ID_LINK_TAB:
				addLinkTableColDef(querySB);

			default:
				break;
		}

	}



	private void addLinkTableColDef(StringBuffer querySB) {
		addCreateTableColumnPart(querySB, LINK_LINK_ID_COL, m_connection.getTypeUtil().getInteger(), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_TYPE_ID_COL, m_connection.getTypeUtil().getInteger(), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_SRC_ID_COL,  m_connection.getTypeUtil().getInteger(), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, LINK_DEST_ID_COL, m_connection.getTypeUtil().getInteger(), false);
		addColDefSeparator(querySB);
		addCreateTableColumnPart(querySB, ORDER_COL,	    m_connection.getTypeUtil().getInteger(), false);
	}

	private void addMultiplePKPart(StringBuffer querySB, String pkName, boolean primarykey, String... columns) {
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
		if (primarykey) {
			querySB.append(" PRIMARY KEY(");
		}
		else {
			if (m_connection.isHSQL())
				querySB.append(" UNIQUE(");
			else 
				querySB.append(" UNIQUE KEY(");
		}
		for (int i = 0; i < columns.length; i++) {
			querySB.append(columns[i]);
			if (i != columns.length - 1)
				querySB.append(",");
		}
		querySB.append(")");
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

		return getEndQuery(sb);
	}

	private static StringBuffer getInsertQuery(String tableName, boolean addValuesParameter, String... colToSets) {

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
		if (addValuesParameter) {
			sb.append(" values (");
			for (int i = 0; i < colToSets.length; i++) {
				sb.append("?");

				if (i != colToSets.length - 1) {
					sb.append(", ");
				}
			}
			sb.append(")");
		}
		return sb;
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
		StringBuffer sb = new StringBuffer();
		addSelectPart(sb, distinct, tableName, columns);
		return sb;
	}

	private static void addSelectPart(StringBuffer sb, boolean distinct, String tableName,  String... columns) {
		sb.append(SELECT_PART);
		if (distinct)
			sb.append(DISTINCT_PART);
		for (int i = 0; i < columns.length; i++) {
			sb.append(columns[i]);
			if (i != columns.length - 1)
				sb.append(", ");
		}
		if (columns.length == 0)
			sb.append("*");
		sb.append(FROM_PART);
		if (tableName != null) {
			sb.append(tableName);
			sb.append(" ");
		}
	}

	private String getEndCreateTableQuery(StringBuffer querySB) {
		querySB.append(")");

		return getEndQuery(querySB);
	}

	private boolean tableExist(int... table) throws ModelVersionDBException {
		for (int i = 0; i < table.length; i++) {
			if(!tableExist(table[i]))
				return false;
		}
		return true;
	}

	private boolean tableExist(int table) throws ModelVersionDBException {
		TableInfo ti = getMetadata().getOrCreateTableInfo(table);
		if (ti.created)
			return true;
		checkConnection();
		ResultSet ps = null;
		try {
			String query = getQuery(query(SELECT, SQL_STRING, P0, FROM, table), args("count(*)"));
			ps   = executeQueryWithScrollable(query, false);
			ti.created = true;
			return true; // the table who has the name = table exists
		} catch (SQLException e) {
			if (e.getErrorCode() == m_connection.getTableNotExistErrorCode())
				return false; // the table who has the name = table doesn't exists

			throw new ModelVersionDBException(e);
		} finally {
			close(ps);
		}
	}

	public synchronized void deleteObject(int objectId) throws ModelVersionDBException {
		deleteSingleObject(objectId);
	}

	private boolean deleteSingleObject(int objectId) throws ModelVersionDBException {
		checkConnection();

		checkObjectIdParam(objectId);
		boolean clearInstances = false;
		beginInternalTransaction();

		try {
			checkTableExist(ID_OBJ_TAB);

			if (isLinkType(objectId)) {
				//delete all links instance for this link type
				StringBuffer querySB = getBeginDeleteQuery(LINK_TAB);
				addWherePart(querySB);
				addEqualPart(querySB, LINK_TYPE_ID_COL, objectId);
				String query = getEndQuery(querySB);
				executeUpdate(query);
			}

			if (isLink(objectId)) {
				// delete link id in link table
				StringBuffer querySB = getBeginDeleteQuery(LINK_TAB);
				addWherePart(querySB);
				addEqualPart(querySB, LINK_LINK_ID_COL, objectId);
				String query = getEndQuery(querySB);
				executeUpdate(query);
			}

			if (isType(objectId)) {
				deleteTable(objectId, false);

				StringBuffer querySB = getBeginDeleteQuery(OBJ_TAB);
				addWherePart(querySB);
				addEqualPart(querySB, OBJ_TYPE_TYPE_ID_COL, objectId);
				String query = getEndQuery(querySB);
				executeUpdate(query);
			}

			Iterator iter = getMetadata()._tables.values().iterator();
			String query;
			// Delete all row where object exist.
			while (iter.hasNext()) {
				TableInfo ti = (TableInfo) iter.next();
				if (!ti.created) {
					continue;
				}
				if (ti._attributes == null) {
					continue;
				}
				StringBuffer querySB = getBeginDeleteQuery(getTypeTabName(ti.typeId));
				addWherePart(querySB);
				boolean addOr = false;
				for (int i = 0; i < ti._attributes.length; i++) {
					if (ti._attributes[i].id) {
						if (addOr) addOrPart(querySB);
						addEqualPart(querySB, ti._attributes[i].name, objectId);
						addOr = true;
					}
				}
				if (addOr) {
					query = getEndQuery(querySB);
					executeUpdate(query);
				}
			}

			checkTableExist(ID_FREE_ID_TAB);
			query = getInsertQuery(FREE_ID_TAB, new Assignment(FREE_ID_COL, objectId));
			executeUpdate(query);

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
		return clearInstances;
	}

	public int[] getOutgoingLinksTypeId(int objectId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_TYPE_ID_COL, LINK_SRC_ID_COL, objectId, false);
	}

	public int[] getOutgoingLinksTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_TYPE_ID_COL, LINK_SRC_ID_COL, objectId, false);
	}

	public int[] getIncomingLinksTypeId(int objectId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_TYPE_ID_COL, LINK_DEST_ID_COL, objectId, false);
	}

    @Override
    public <T> DBIteratorID<T> getIncomingLinks(Class<T> kind, int linkTypeId, int objectDestId) throws ModelVersionDBException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> DBIteratorID<T> getLinksId(Class<T> kind, int type, int srcId, int destId) throws ModelVersionDBException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

        

	private final static StringBuffer getBeginDeleteQuery(String tableName) {
		StringBuffer sb = new StringBuffer("DELETE FROM ");
		sb.append(tableName);
		sb.append(" ");

		return sb;
	}



	private void deleteTable(int tableId, boolean view) throws ModelVersionDBException {
		try {
			String tableName = getTypeTabName(tableId);

			StringBuffer querySB = new StringBuffer();
			querySB.append("DROP ");
			if (view)
				querySB.append("VIEW ");
			else
				querySB.append("TABLE ");
			if (!m_connection.isOracle())
					querySB.append(" IF EXISTS ");
			querySB.append(tableName);

			executeUpdate(getEndQuery(querySB));

			getMetadata().removeTable(tableId);
		} catch (SQLException e) {
			if (m_connection.isOracle() && (e.getErrorCode() == m_connection.getTableNotExistErrorCode()))
				return;

			throw new ModelVersionDBException(e);
		}
	}


	public boolean objExists(int objectId) throws ModelVersionDBException {
		checkConnection();

		checkObjectIdParam(objectId);
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, OBJ_TAB, OBJ_OBJ_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return (rs.next());

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return false;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}


	private boolean isTableNotExistError(SQLException e) {
		return (e.getErrorCode() == m_connection.getTableNotExistErrorCode());
	}

	private void addEqualPart(StringBuffer querySB, String colName, Object value) {
		querySB.append(colName);
		querySB.append(" = ");
		querySB.append(m_connection.getTypeUtil().getSQLValue(value));
	}
	private void addNotEqualPart(StringBuffer querySB, String colName, Object value) {
		querySB.append(colName);
		querySB.append(" <> ");
		querySB.append(m_connection.getTypeUtil().getSQLValue(value));
	}

	private void addJointureEqualPart(StringBuffer querySB, String colName, String colName2) {
		querySB.append(colName);
		querySB.append(" = ");
		querySB.append(colName2);
	}

	private void addEqualPart(StringBuffer querySB, String colName) {
		querySB.append(colName);
		querySB.append(" = ? ");
	}

	private String getEndQuery(ConnectionDef connection, StringBuffer querySB) {
		if (connection != null && !connection.isOracle())
			querySB.append(";");

		return querySB.toString();
	}
	
	private String getEndQuery(StringBuffer querySB) {
		if (m_connection != null && !m_connection.isOracle())
			querySB.append(";");

		return querySB.toString();
	}

	private static void addWherePart(StringBuffer querySB) {
		querySB.append(WHERE_PART);
	}

	private static void addAndPart(StringBuffer querySB) {
		querySB.append(SQLConstants.AND_PART);
	}

	private static void addOrPart(StringBuffer querySB) {
		querySB.append(SQLConstants.OR_PART);
	}

	public int[] getObjects(int typeId) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);

		return getIds(ID_OBJ_TYPE_TAB, OBJ_TYPE_OBJ_ID_COL, OBJ_TYPE_TYPE_ID_COL, typeId, false);
	}

	public void saveLinkObject(int linkTypeId, int linkId, int sourceId, int destId, int[] compatibleVersions)
	throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(linkTypeId);
		checkSrcIdParam(sourceId);
		checkDestIdParam(destId);

		if (isType(linkId)) {
			checkTypeParam(sourceId);
			checkTypeParam(destId);
		}
		if (linkId!=linkTypeId) {
			//if (!isLink(linkTypeId)) {
			//	throw new IllegalArgumentException("Not a link type: "+linkTypeId);
			//}
			//int typeSourceId = getLinkSrc(linkTypeId);
			//int typeDestId = getLinkDest(linkTypeId);
//			if (!isCompatibleLink(sourceId, typeSourceId)) {
//				m_logger.log(Logger.ERROR,"LinkTypeId:"+linkTypeId+"("+typeSourceId+"-->"+typeDestId+")");
//				throw new IllegalArgumentException(
//						NLS.bind("Source link {0} is not compatible with source type {1}", sourceId, typeSourceId));
//			}
//			if (!isCompatibleLink(destId, typeDestId)) {
//				m_logger.log(Logger.ERROR,"LinkTypeId:"+linkTypeId+"("+typeSourceId+"-->"+typeDestId+")");
//				throw new IllegalArgumentException(
//						NLS.bind("Destination link {0} is not compatible with destination type {1}.", destId, typeDestId));
//			}
		}
		beginInternalTransaction();

		try {
			checkTableExist(ID_LINK_TAB);

			int nextOrder = getLastOrder(linkTypeId, sourceId);
			if (linkExists(linkId)) {

			} else {
				String querySB = getInsertQuery(LINK_TAB,
						new Assignment(LINK_LINK_ID_COL, linkId),
						new Assignment(LINK_TYPE_ID_COL, linkTypeId),
						new Assignment(LINK_SRC_ID_COL, sourceId),
						new Assignment(LINK_DEST_ID_COL, destId),
						new Assignment(ORDER_COL, nextOrder));
				executeUpdate(querySB);
			}
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public int createLinkIfNeed(int linkTypeId, int sourceId, int destId) throws ModelVersionDBException {
		int[] links = getLinksId(linkTypeId, sourceId, destId);
		if (links.length != 0) return links[0];
		int linkId = createLocalIdentifier();
		saveLinkObject(linkTypeId, linkId, sourceId, destId, null);
		return linkId;
	}

	private boolean isCompatibleLink(int sourceId, int typeSourceId) throws ModelVersionDBException {
		return isInstanceof(sourceId, typeSourceId);
	}

	private int getLastOrder(int typeId, int srcId)
			throws SQLException {
		StringBuffer querySB;
		String query;
		ResultSet rs;
		int lastOrder = -1;
		querySB = getBeginSelectQuery(false, LINK_TAB, maxFunction(ORDER_COL));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
		addAndPart(querySB);
		addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
		query = getEndQuery(querySB);
		rs = executeQuery(query);
		if (rs.next()) {
			lastOrder = rs.getInt(1);
		}
		close(rs);

		return lastOrder;
	}

	private static String getResetSequenceQuery(String lastRevSeqName, int idx) {
		StringBuffer querySB = new StringBuffer();
		querySB.append("ALTER SEQUENCE " + lastRevSeqName + " RESTART WITH " + idx);

		return querySB.toString();
	}


	private StringBuffer getBeginSelectQuery(boolean distinct, String tableName,
			List<String> columnsList) {
		return getBeginSelectQuery(distinct, tableName, columnsList.toArray(new String[columnsList.size()]));
	}

	private String maxFunction(String column) {
		return "max(" + column + ") ";
	}

	public synchronized void deleteLink(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		beginInternalTransaction();

		try {
			checkTableExist(ID_LINK_TAB);

			if (!linkExists(linkId))
				throw new IllegalArgumentException("Link with id = " + linkId
						+ " doesn't exists.");

			// delete object state
			if (objExists(linkId)) {
				deleteObject(linkId);
			}




			// Note that we don't remove link source or destination objects

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private void checkLinkIdParam(int linkId) {
		if (linkId == -1)
			throw new IllegalArgumentException("Link id cannot be null.");
	}

	private void checkObjectIdParam(int objId) throws ModelVersionDBException {
		if (objId == -1)
			throw new IllegalArgumentException("Object id cannot be null.");
		if (getUniqueIdentifier(objId) == null) {
			throw new IllegalArgumentException("Object id not register.");
		}
	}

	public int getLinkNumber(int typeId, int srcId) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {

			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB,
					"count(" + LINK_DEST_ID_COL + ")");
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			String query = getEndQuery(querySB);

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
			close(rs);
		}
	}

	public ID3 getLinkDestID3(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");

		return getIdForLink(LINK_DEST_ID_COL, LINK_LINK_ID_COL, linkId);
	}



	public int getLinkDest(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
			//throw new IllegalArgumentException(
			//		"There is no link with id = " + linkId + " in database");
			return -1;

		return getIdForLink_(LINK_DEST_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public int getLinkOrder(int linkId) throws ModelVersionDBException {
		return getId(ID_LINK_TAB, LINK_LINK_ID_COL, ORDER_COL, linkId);
	}

	/**
	 * P0 : source, dest ou type
	 * P1 : id, source, dest, type
	 * P2 : valueur de equal
	 */
	static final int[] id3ForLink = {
			SELECT, T2, ID_OBJ_CADSE_COL,
		     T1, ID_OBJ_TYPE_ID_COL,
	         T0,  P0,
	FROM, ID_LINK_TAB, T0,
		  ID_OBJ_TAB, T1,
		  ID_OBJ_TAB, T2,
	WHERE,
		  T0, COL, P1, EQUAL, P2, AND,
		  T1, ID_OBJ_OBJ_ID_COL, EQUAL, T0, COL, P0, AND,
		  T1, ID_OBJ_TYPE_ID_COL, EQUAL, T2, ID_OBJ_OBJ_ID_COL
	};
	private ID3 getIdForLink(String selColName, String equalColName,
			int equalId) throws ModelVersionDBException {
		ResultSet rs = null;
		try {
			String query = getQuery(id3ForLink,
						  args(selColName, equalColName, equalId));
			rs = executeQuery(query);
			return toId3(rs);

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private int getIdForLink_(String selColName, String equalColName,
			int equalId) throws ModelVersionDBException {
		ResultSet rs = null;
		try {
			String query =
				getQuery(query(SELECT, T0,  COL, P0,
					FROM, ID_LINK_TAB, T0,
					WHERE, T0, COL, P1, EQUAL, P2),
						  args(selColName, equalColName, equalId));

			rs = executeQuery(query);
			return rs.next()? rs.getInt(1):NULL_ID;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private ID3 toId3(ResultSet rs) throws SQLException {
		if (rs.next()) {
			return new ID3(rs.getInt(1), rs.getInt(2), rs.getInt(3));
		}
		return NULL_ID3;
	}

	public ID3 getLinkSrcID3(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");

		return getIdForLink(LINK_SRC_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public int getLinkSrc(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
//			throw new IllegalArgumentException(
//					"There is no link with id = " + linkId + " in database");
			return -1;

		return getIdForLink_(LINK_SRC_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public Map<Integer, Object> getLinkState(int linkId)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
			throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

		try {
			beginInternalTransaction();
			Map<Integer, Object> resultMap = getObjectState(linkId);
			commitInternalTransaction();

			return resultMap;
		} catch (Exception e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	private Map<Integer, Object> getState(int objId, int... typeId)
			throws ModelVersionDBException, SQLException {


		// get values in DB
		StringBuffer querySB = getBeginSelectQuery(false, null);
		boolean first = true;
		for (int t : typeId) {
			if (!first)
				querySB.append(',');
			else
				first = false;
			querySB.append(getTypeTabName(t));

		}
		addWherePart(querySB);
		first = true;
		for (int t : typeId) {
			if (!first)
				addAndPart(querySB);
			else
				first = false;
			querySB.append(getTypeTabName(t));
			querySB.append('.');
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objId);
		}

		String query = getEndQuery(querySB);

		Map<Integer, Object> resultMap = new HashMap<Integer, Object>();
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (!rs.next())
				return resultMap;

			for (int t : typeId) {
				TableInfo ta = getMetadata().getTableInfo(t, true);
				for (TableAttribute2 attr : ta.getAttributes()) {
					String colName = attr.getColumn();
					String attrType = attr.getType();
					Object value = getValue(rs, colName, attrType);
					resultMap.put(attr.attributeId, value);
				}
			}
		} finally {
			close(rs);
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

		// must be after int type recognizer
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


	public ID3 getLinkType(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		if (!linkExists(linkId))
			throw new IllegalArgumentException(
					"There is no link with id = " + linkId + " in database");

		return getIdForLink(LINK_TYPE_ID_COL, LINK_LINK_ID_COL, linkId);
	}

	public Object getLinkValue(int linkId, int attrID)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkAttrNameParam(attrID);
		beginInternalTransaction();

		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			Object result = getValue(linkId, attrID);
			commitInternalTransaction();
			return result;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	/**
	 * @param attrID
	 * @throws ModelVersionDBException
	 */
	private void checkAttrNameParam(int attrID) throws ModelVersionDBException {
		if (attrID == -1 || !isAttributeDefinition(attrID))
			throw new IllegalArgumentException("Attribute name no exits: "+attrID);
	}

	private Object getValue(int objId, int attr)
			throws ModelVersionDBException, SQLException {

		TableAttribute2 attrJO = getMetadata().getAttribute(attr, true);
		String tableName = getTypeTabName(attrJO.getTypeId());

		// manage LAST revision case
		//rev = manageRevNb(objId, rev, false);


		// get values in DB
		StringBuffer querySB = getBeginSelectQuery(false, tableName, attrJO
				.getColumn());
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objId);
		String query = getEndQuery(querySB);
		ResultSet rs = null;
		try {
			rs = executeQuery(query);
			if (!rs.next())
				throw new IllegalArgumentException("Cannot find value of attribute " + attr);

			return getValue(rs, attrJO.getColumn(), attrJO.getType());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Cannot find value of attribute " + attr,e);
		} finally {
			close(rs);
		}
	}

        @Override
        public <T> DBIteratorID<T> getLinks(Class<T> kind) throws ModelVersionDBException {
            throw new UnsupportedOperationException("Not supported yet.");
        }




	public DBIteratorID getLinks() throws ModelVersionDBException {
		return getIdsForLink();
	}



	public synchronized Map<Integer, Object> getObjectState(int objectId)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);

		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");
			beginInternalTransaction();

			int[] typeId = getAllObjectTypes(objectId);

			Map<Integer, Object> resultMap = getState(objectId, typeId);

			commitInternalTransaction();

			return resultMap;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public Object getObjectValue(int objectId, int attrID)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkAttrNameParam(attrID);
		beginInternalTransaction();

		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");

			Object value = getValue(objectId, attrID);
			commitInternalTransaction();

			return value;
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public boolean linkExists(int linkId) throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		ResultSet rs = null;
		try {
			if (!tableExist(ID_LINK_TAB))
				return false;

			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, "*");
			addWherePart(querySB);
			addEqualPart(querySB, LINK_LINK_ID_COL, linkId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query);

			return (rs.next());

		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return false;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	public boolean linkExists(int typeId, int srcId, int destId)
			throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkDestIdParam(destId);

		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);
			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query);

			if (!rs.next())
				return false;

			return (rs.getInt(1) > 0);

		} catch (SQLException e) {
			if (isTableNotExistError(e))
				return false;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	public synchronized void setLinkState(int linkId, Map<Integer, Object> stateMap)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);

		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			if ((stateMap == null) || (stateMap.isEmpty()))
				throw new IllegalArgumentException(
						"State map musn't be null or empty");

			beginInternalTransaction();
			try {
				storeState(linkId, stateMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			} catch (IOException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
		}
	}

	public void setLinkValue(int linkId, int attrID, Object value)
			throws ModelVersionDBException {
		checkConnection();
		checkLinkIdParam(linkId);
		checkAttrNameParam(attrID);

		try {
			if (!linkExists(linkId))
				throw new IllegalArgumentException(
						"There is no link with id = " + linkId + " in database");

			beginInternalTransaction();
			updateValue(linkId, attrID, value);
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
		}
	}

	private void updateValue(int objectId, int attrID, Object value) throws ModelVersionDBException, SQLException {
		TableAttribute2 attrJO;
		try {
			attrJO = getMetadata().getAttribute(attrID, true);
		} catch (IllegalArgumentException e) {
			throw e;
		}

		// check attribute exist
		String newAttrType = null;
		try {
			newAttrType = m_connection.getTypeUtil().getSQLType(value);
		} catch (ModelVersionDBException e) {
			throw new ModelVersionDBException("Attribute " + attrID
					+ " has no serializable type.", e);
		}
		String attrType = attrJO.getType();
		if (m_connection.getTypeUtil().incompatibleTypes(attrType, newAttrType)) {
			if (m_connection.getTypeUtil().migratableToNewType(attrType, newAttrType)) {
				migrateCol(attrJO.getTypeId(), attrID, newAttrType, attrJO.getColumn());
			} else
				throw new ModelVersionDBException("Found persist type "
					+ attrType + " and new type " + newAttrType
					+ " for attribute " + attrID + " of type " + attrJO.getTypeId());
		}

		String typeTabName = getTypeTabName(attrJO.getTypeId());
		createObjectRowIfNeed(objectId, typeTabName);
		// update attribute value in DB
		StringBuffer querySB = getBeginUpdateQuery(typeTabName, new Assignment(attrJO.getColumn()));
		addWherePart(querySB);
		addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
		String query = getEndQuery(querySB);
		PStatement ps = null;
		try {
			ps = createPreparedStatement(query);
			setValue(ps, 1, value);
			ps.executeUpdate();
		} catch (IllegalArgumentException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} finally {
			close(ps);
		}
	}

	public synchronized void setObjectState(int objectId, Map<Integer, Object> stateMap)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);

		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId
								+ " in database");

			if ((stateMap == null) || (stateMap.isEmpty()))
				throw new IllegalArgumentException(
						"State map musn't be null or empty");

			beginInternalTransaction();
			try {
				storeState(objectId, stateMap);
			} catch (IllegalArgumentException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			} catch (IOException e) {
				rollbackInternalTransaction(e);
				throw new ModelVersionDBException(e);
			}
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
		}
	}

	public synchronized void setObjectValue(int objectId, int attrID, Object value)
			throws ModelVersionDBException {
		checkConnection();
		checkObjectIdParam(objectId);
		checkAttrNameParam(attrID);

		try {
			if (!objExists(objectId))
				throw new IllegalArgumentException(
						"There is no object with id = " + objectId + " in database");



			beginInternalTransaction();
			updateValue(objectId, attrID, value);
			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
		}
	}

	private DBIteratorID<ID3> getIdsForLink()
			throws ModelVersionDBException {
		if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return executeQueryID3(query(SELECT, ID_OBJ_CADSE_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
				ORDER_BY, ID_LINK_SRC_ID_COL, ID_LINK_TYPE_ID_COL, ID_ORDER_COL), args());
	}

	private DBIteratorID<ID3> getIdsForLink(int fEqualColName,
			int fEqualId)
			throws ModelVersionDBException {
		if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return executeQueryID3(query(SELECT, ID_OBJ_CADSE_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
					AND, COL, P0, EQUAL, P1), args(fEqualColName, fEqualId));
	}

	private DBIteratorID<ID3> getIdsForLink(String fEqualColName,
			int fEqualId, String sEqualColName, int sEqualId)
			throws ModelVersionDBException {
		if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return executeQueryID3(query(SELECT, ID_OBJ_CADSE_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
					AND, P0, EQUAL, P1,
					AND, P2, EQUAL, P3,
				ORDER_BY, ID_LINK_SRC_ID_COL, ID_LINK_TYPE_ID_COL, ID_ORDER_COL),args(fEqualColName, fEqualId,
					sEqualColName, sEqualId));
	}



        @Override
	public int[] getOutgoingLinksAggregation(int objId) throws ModelVersionDBException {
		try {
			String query = getQuery(
			query(SELECT, T1, ID_LINK_LINK_ID_COL,
					FROM, CadseGPackage.LocalId.LINK_TYPE, T0, ID_LINK_TAB, T1,
					WHERE, T1, ID_LINK_SRC_ID_COL, EQUAL, P0,
					AND, T0, CadseGPackage.LocalId.LINK_TYPE_at_AGGREGATION,
					AND, T0, ID_PERTYPE_OBJ_ID_COL, EQUAL, T1,  ID_LINK_TYPE_ID_COL),
						   args(objId));
			return getIds(query, false);
		}
		catch(SQLException e){
			throw new ModelVersionDBException(e);
		}
	}

	public int[] getOutgoingLinksAggregation2(int objId) throws ModelVersionDBException {
		try {
			String query = getQuery(query(SELECT, T1, ID_LINK_LINK_ID_COL,
					FROM, CadseGPackage.LocalId.LINK_TYPE, T0, ID_LINK_TAB,T1,
					WHERE, T1, ID_LINK_SRC_ID_COL, EQUAL, P0,
					AND, T0, CadseGPackage.LocalId.LINK_TYPE_at_AGGREGATION,
					AND, T0, ID_PERTYPE_OBJ_ID_COL, EQUAL, T1,  ID_LINK_TYPE_ID_COL),
						   args(objId));
			return getIds(query, false);
		}
		catch(SQLException e){
			throw new ModelVersionDBException(e);
		}
	}

	private DBIteratorID<ID3> executeQueryID3(int[] q, Object[] args)
			throws ModelVersionDBException {
		checkConnection();

		try {
			return new DBIteratorIDImpl.IterID3(this, executeQuery(getQuery(q, args)));

		} catch (SQLException e) {
			throw new ModelVersionDBException(querytoString(q, args),e);
		}
	}

        private DBIteratorID<LinkInfo> executeQueryLinkInfo(int[] q, Object[] args)
			throws ModelVersionDBException {
		checkConnection();

		try {
			return new DBIteratorIDImpl.IterLinkInfo(this, executeQuery(getQuery(q, args)));

		} catch (SQLException e) {
			throw new ModelVersionDBException(querytoString(q, args),e);
		}
	}

         private DBIteratorID<LinkInfoPlus> executeQueryLinkInfoPlus(int[] q, Object[] args)
			throws ModelVersionDBException {
		checkConnection();

		try {
			return new DBIteratorIDImpl.IterLinkInfoPlus(this, executeQuery(getQuery(q, args)));

		} catch (SQLException e) {
			throw new ModelVersionDBException(querytoString(q, args),e);
		}
	}

	public int[] query(int... q) {
		return q;
	}

	public Object[] args(Object... a) {
		return a;
	}

	public String getQuery(int[] elts, Object[] args) throws ModelVersionDBException, SQLException {
		StringBuffer sb = new StringBuffer();
		Metadata m = getMetadata();
		boolean nextCol = true;
		boolean mustAppendVir = false;
		boolean addvir = false;

		int state = 0;
		for (int i = 0; i < elts.length; i++) {
			int c = elts[i];
			if (c == FROM) {
				nextCol = false;
			} else if (c == SELECT || c == OPEN_PAR || c == ORDER_BY || c == WHERE || c == CLOSE_PAR) {
				nextCol = true;
			}
			if (c == COL) {
				state = COL; continue;
			}
			if (c == SQL_STRING) {
				state = SQL_STRING; continue;
			}
			if (c == SELECT || c == FROM || c == OPEN_PAR || c == ORDER_BY) {
				mustAppendVir = true;
				addvir =false;
			} else if (c == WHERE || c == CLOSE_PAR|| c == UNION) {
				mustAppendVir = false;
			}

			if (c >= P0 && c <= P9) {
				Object a = args[c-P0];
				if (state == SQL_STRING) {
					if (mustAppendVir && addvir)
						sb.append(", ");
					sb.append(a);
					addvir = true;
					state = 0;
					continue;
				}
				else if (state == COL) {
					if (a instanceof String) {
						if (mustAppendVir && addvir)
							sb.append(", ");
						sb.append(a);
						addvir =true;
						state = 0;
						continue;
					}
					c = (Integer) a;
				} else {
					if (mustAppendVir && addvir)
						sb.append(", ");
					sb.append(m_connection.getTypeUtil().getSQLValue(a));
					addvir =true;
					state = 0;
					continue;
				}
			}
			state = 0;

			if (c < -1024) {
				if (c >= T0 && c <= T9) {
					if (nextCol) {
						if (mustAppendVir && addvir)
							sb.append(", ");
					} else {
						sb.append(" ");
					}
				}
				addvir = false;
				sb.append(SQL_NAME[decal_sql+c]);

				if (c >= T0 && c <= T9) {
					addvir = !nextCol;
					if (nextCol) {
						sb.append(".");
					}
				}
				continue;
			}

			if (mustAppendVir && addvir)
				sb.append(", ");

			addvir = true;
			if (c < -32) {
				sb.append(ATTRIBUTES_COLS_NAME[1024+c]);
			}
			else if (c < 0) {
				sb.append(TABS[32+c]);
			} else if (nextCol) {
				TableAttribute2 at = m.getAttribute(c);
				if (at == null)
					throw new ModelVersionDBException("bad expression : not found attribute "+c);
				sb.append(at.getColumn());
			} else {
				TableInfo at = m.getTableInfo(c,true);
				if (at == null)
					throw new ModelVersionDBException("bad expression : not found table "+c);
				sb.append(at.getTableName());
			}
		}
		return printDebug(elts, args, getEndQuery(sb));
	}

	private String printDebug(int[] elts, Object[] args, String query) {
		m_logger.log(Logger.DEBUG, querytoString(elts, args));
		m_logger.log(Logger.DEBUG, query);
		return query;
	}

	private String querytoString(int[] elts, Object[] args) {
		StringBuffer sb = new StringBuffer();

		boolean nextCol = true;
		for (int i = 0; i < elts.length; i++) {
			int c = elts[i];
			if (c < -1024) {
				sb.append(SQL_NAME[decal_sql+c]);
				if (c >= P0 && c <= P9) {
					Object a = args[c-P0];
					sb.append("(").append(a).append(") ");
				}
				if (c >= T0 && c <= T9) {
					if (nextCol)
						sb.append(".");
				}
			}
			else if (c < -32) {
				sb.append(ATTRIBUTES_COLS_NAME[1024+c]).append(" ");
			}
			else if (c < 0) {
				sb.append(TABS[32+c]).append(" ");
			} else if (nextCol) {
				TableAttribute2 attr = getMetadata().getAttribute(c);
				if (attr == null)
					sb.append(" <Attribute? ").append(c).append(">");
				else {
					String attrName = null;
					try {
						attrName = getName(c);
						String tableName = getName(attr.parent.typeId);
						sb.append(" <Attribute ").append(tableName == null ? attr.parent.getTableName() : tableName).
						append(".").append(attrName == null ? attr.getColumn(): attrName).append("> ");
					} catch (ModelVersionDBException e) {
						sb.append(" <Attribute ").append(attr.parent.getTableName()).
						append(".").append(attrName == null ? attr.getColumn(): attrName).append("> ");
					}
				}
			} else {
				TableInfo attr = (TableInfo) getMetadata()._tables.get(c);
				if (attr == null)
					sb.append(" <Table? ").append(c).append("> ");
				else {
					String tableName = null;
					try {
						tableName = getName(c);

						sb.append(" <Table ").append(tableName == null ? c : tableName).
						append("> ");
					} catch (ModelVersionDBException e) {
						sb.append(" <Table ").append(c).append("> ");
					}
				}
			}
			if (c == FROM)
				nextCol = false;
			else if (c == WHERE )
				nextCol = true;


		}
		// TODO Auto-generated method stub
		return sb.toString();
	}

	public int[] getLinks(int typeId) throws ModelVersionDBException {
		checkConnection();
		checkTypeParam(typeId);
		ResultSet rs = null;
		try {
			if (!tableExist(ID_LINK_TAB))
				return EMPTY_IDS;

			List<String> columns = new ArrayList<String>();
			columns.add(LINK_LINK_ID_COL);
			if (m_connection.isHSQL()) {
				columns.add(LINK_SRC_ID_COL);
				columns.add(ORDER_COL);
			}

			StringBuffer querySB = getBeginSelectQuery(true, LINK_TAB, columns);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addOrderByPart(querySB, LINK_SRC_ID_COL, ORDER_COL);
			String query = getEndQuery(querySB);

			rs = executeQuery(query);

			return getIntArray(rs, true);

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private void addOrderByPart(StringBuffer querySB, String... columns) {
		if ((columns == null) || (columns.length == 0))
			return;

		querySB.append(ORDER_BY_PART);
		for (int i = 0; i < columns.length; i++) {
			if (i > 0)
				querySB.append(", ");
			querySB.append(columns[i]);
		}
	}


	private void checkTypeParam(int typeId) throws ModelVersionDBException {
		if (typeId == -1)
			throw new IllegalArgumentException("Type id cannot be null.");
		//if (!(objExists(typeId) && isType(typeId)))
		//	throw new IllegalArgumentException("Type id not exist.");

	}

	private void checkTypeParam(int[] typeId, int objectId) throws ModelVersionDBException {
		if (typeId == null || typeId.length == 0)
			throw new IllegalArgumentException("Type id cannot be null.");
//		for (int id : typeId) {
//			if (objectId != id && !objExists(id) && !isType(id))
//				throw new IllegalArgumentException("Type id not exists");
//		}
	}



	private void checkDestIdParam(int destId) throws ModelVersionDBException {
		if (destId == -1)
			throw new IllegalArgumentException("Source object id cannot be null.");
		//if (!objExists(destId))
		//	throw new IllegalArgumentException("Source object id cannot be null.");
	}

	private void checkSrcIdParam(int srcId) throws ModelVersionDBException {
		if (srcId == -1)
			throw new IllegalArgumentException("Destination object id cannot be null.");
		//if (!objExists(srcId))
		//	throw new IllegalArgumentException("Source object id cannot be null.");
	}

	private int[] getTypes() throws ModelVersionDBException {
		ResultSet rs = null;
		try {
			if (!tableExist(ID_TYPE_TAB))
				return EMPTY_IDS;

			StringBuffer querySB = getBeginSelectQuery(false, TYPE_TAB, TYPE_TYPE_ID_COL);
			String query = getEndQuery(querySB);

			executeQuery(query);
			return getIntArray(rs, false);
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	public synchronized void clear() throws ModelVersionDBException {
		checkConnection();
		beginInternalTransaction();

		//if (!m_allowAdmin)
		//	throw new SecurityException("Administration of this service is not allowed !");

		if (!tableExist(ID_UUID_TAB))
			return;

		for (int type : getTypes()) {
			try {
				deleteTable(type, false);
			} catch (ModelVersionDBException e) {
				m_logger.log(Logger.ERROR, "Delete table " + getTypeTabName(type) + " failed.", e);
			}
		}

		forceDeleteTable(ID_ALL_OBJ_TYPE_TAB, true);
		forceDeleteTable(ID_TYPE_SUPER_TAB, false);
		forceDeleteTable(ID_TYPE_EXT_TAB, false);
		forceDeleteTable(ID_OBJ_TYPE_TAB, false);
		forceDeleteTable(ID_OBJ_TAB, false);
		forceDeleteTable(ID_GEN_TAB, false);
		forceDeleteTable(ID_TYPE_TAB, false);
		forceDeleteTable(ID_ATTR_TAB, false);
		forceDeleteTable(ID_LINK_TAB, false);
		forceDeleteTable(ID_FREE_ID_TAB, false);
		forceDeleteTable(ID_UUID_TAB, false);
		forceDeleteTable(ID_ALL_TYPE_SUPER_TAB, false);
		forceDeleteTable(ID_ALL_EXT_TAB, false);

		commitInternalTransaction();
	}


	private void forceDeleteTable(int table, boolean view) {
		try {
			if (tableExist(table)) {
				deleteTable(table, view);
			}
		} catch (ModelVersionDBException e) {
			m_logger.log(Logger.ERROR, "Delete table " + table + " failed.", e);
		}
	}

	private void checkConnection() throws ModelVersionDBException {
		if (!isConnected())
			throw new ModelVersionDBException("No connection has been opened.");
	}





	public void reOrderLinks(int typeId, int srcId, int... linkIds)
			throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		beginInternalTransaction();

		if ((linkIds == null) || (linkIds.length < 2))
			throw new IllegalArgumentException("Link list must contain at least two elements.");

		try {
			checkTableExist(ID_LINK_TAB);

			reOrderLinksInternal(typeId, srcId, linkIds);


			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch(ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
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
	private void reOrderLinksInternal(int typeId, int srcId,
			int... linkIds) throws SQLException, ModelVersionDBException {

		// retrieve current link order
		StringBuffer querySB;

		checkReOrder(typeId, srcId, linkIds);


		// update link order
		querySB = getBeginUpdateQuery(LINK_TAB,
				new Assignment(ORDER_COL));
		addWherePart(querySB);
		addEqualPart(querySB, LINK_LINK_ID_COL); // set ? as value

		String query = getEndQuery(querySB);
		PStatement prepare = null;
		try {
			prepare = createPreparedStatement(query);
		for (int i = 0; i < linkIds.length; i++) {
			setValue(prepare, 1, i );
			setValue(prepare, 2, linkIds[i]);
				prepare.executeUpdate();
			}
			} catch (IOException e) {
				throw new ModelVersionDBException(e);
		} finally {
			close(prepare);
		}
        }

	private int[][] setIds(int tabId,
			String whereCol,
			String selectCol, int id, boolean useOrder, boolean addOnly,
			int... newIds) throws SQLException, IOException, ModelVersionDBException {
		int[] saveIds = getIds(tabId, selectCol, whereCol, id, useOrder);
		int[] posNewIds = new int[newIds.length];
		int[] posSaveIds = new int[saveIds.length];
		for (int i = 0; i < newIds.length; i++) {
			posNewIds[i] = -1;
		}
		for (int i = 0; i < saveIds.length; i++) {
			posSaveIds[i] = -1;
		}
		int findTuples = 0;
		for (int i = 0; i < saveIds.length; i++) {
			for (int j = 0; j < newIds.length; j++) {
				if (saveIds[i] == newIds[j]) {
					if (posSaveIds[i] != -1)
						throw new IllegalArgumentException();
					if (posNewIds[j] != -1)
						throw new IllegalArgumentException();
					posSaveIds[i] = j;
					posNewIds[j] = i;
					findTuples++;
					break;
				}
			}
		}
		int addedTuples = newIds.length - findTuples;
		int removedTuples = addOnly?0:  saveIds.length - findTuples;
		int[] addedTuplesIds = new int[addedTuples];
		int[] removedTuplesIds = new int[removedTuples];
		int[] tuplesIds = addOnly?saveIds: new int[findTuples];

		int iaddedTuples = 0;
		int iremovedTuples = 0;
		int iTuples = 0;
		String tabName = getTypeTabName(tabId);
		if (!addOnly) {
			for (int i = 0; i < posSaveIds.length; i++) {
				if (posSaveIds[i] == -1) {
					deleteTupleIds(tabName, whereCol, id, selectCol, saveIds[i]);
					removedTuplesIds[iremovedTuples++] = saveIds[i];
				}
			}
		}
		for (int i = 0; i < posNewIds.length; i++) {
			if (posNewIds[i] ==-1) {
				insertTupleIds(tabName, whereCol, id, selectCol, newIds[i]);
				addedTuplesIds[iaddedTuples++] = newIds[i];
			}
			else {
				if (!addOnly)
					tuplesIds[iTuples++] = newIds[i];
			}
		}
		String query;
		if (useOrder) {
					// update link order
				StringBuffer querySB = getBeginUpdateQuery(tabName,
						new Assignment(ORDER_COL));
				addWherePart(querySB);
				addEqualPart(querySB, whereCol);
				addAndPart(querySB);
				addEqualPart(querySB, selectCol);

				query = getEndQuery(querySB);
				PStatement prepare = null;
				try {
					prepare = createPreparedStatement(query);
				for (int i = 0; i < newIds.length; i++) {
					setValue(prepare, 1, i );
					setValue(prepare, 2, id);
					setValue(prepare, 3, newIds[i]);
					try {
						prepare.executeUpdate();
					} catch (IOException e) {
						throw new ModelVersionDBException(e);
					}
				}
				} finally {
					close(prepare);
		                }
		}
		return new int[][]{ tuplesIds, addedTuplesIds, removedTuplesIds};
	}

	private void insertTupleIds(String tabName, String col1, int id1,
			String col2, int id2) throws ModelVersionDBException, SQLException, IOException {

		PStatement ps = null;
		try{
			StringBuffer querySB = getInsertQuery(tabName, true, col1, col2);
			ps = createPreparedStatement(getEndQuery(querySB));
			ps.getStatement().setInt(1, id1);
			ps.getStatement().setInt(2, id2);
			ps.executeUpdate();
			m_logger.log(Logger.INFO, "INSERT "+tabName+" ("+id1+","+id2+")");
		} catch(SQLException e) {
			m_logger.log(Logger.ERROR, "INSERT "+tabName+" ("+id1+","+id2+")",e);
			throw e;
		} catch(IOException e) {
			m_logger.log(Logger.ERROR, "INSERT "+tabName+" ("+id1+","+id2+")",e);
			throw e;
		} finally {
			close(ps);
		}
	}

	private void deleteTupleIds(String tabName, String col1, int id1,
			String col2, int id2) throws ModelVersionDBException, SQLException {
		StringBuffer querySB = getBeginDeleteQuery(tabName);
		addWherePart(querySB);
		addEqualPart(querySB, col1, id1);
		addAndPart(querySB);
		addEqualPart(querySB, col2, id2);
		String query = getEndQuery(querySB);
		executeUpdate(query);
	}

	private void checkReOrder(int typeId, int srcId, int... linkIds)
			throws ModelVersionDBException {
		int[] links = getOutgoingLinks(typeId, srcId)	;
		if (links.length != linkIds.length)
			throw new IllegalArgumentException();
		int[] posLinksIds = new int[linkIds.length];
		int[] posLinksIds2 = new int[links.length];
		for (int i = 0; i < linkIds.length; i++) {
			posLinksIds[i] = -1;
		}
		for (int i = 0; i < links.length; i++) {
			posLinksIds2[i] = -1;
		}
		for (int i = 0; i < linkIds.length; i++) {
			for (int j = 0; j < links.length; j++) {
				if (linkIds[i] == links[j]) {
					if (posLinksIds[i] != -1)
						throw new IllegalArgumentException();
					if (posLinksIds2[j] != -1)
						throw new IllegalArgumentException();
					posLinksIds[i] = j;
					posLinksIds2[j] = i;
					break;
				}
			}
		}
		for (int i = 0; i < links.length; i++) {
			if (posLinksIds2[i] == -1)
				throw new IllegalArgumentException();

		}
		for (int i = 0; i < linkIds.length; i++) {
			if (posLinksIds[i] == -1)
				throw new IllegalArgumentException();

		}
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
					throw new TransactionException("Commit Transaction failed.");
				}
			}
		}

		clearTransaction();
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



	public void deleteLink(int typeId, int srcId, int destId) throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(typeId);
		checkSrcIdParam(srcId);
		checkDestIdParam(destId);

		beginInternalTransaction();

		try {
			checkTableExist(ID_LINK_TAB);

			String typeTableName = getTypeTabName(typeId);

			// delete link states
			StringBuffer querySB = getBeginDeleteQuery(typeTableName + " T");
			addWherePart(querySB);
			querySB.append("EXISTS (");
			querySB.append(getBeginSelectQuery(false, LINK_TAB + " L",
					LINK_LINK_ID_COL).toString());
			addWherePart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);

			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);

			addAndPart(querySB);
			addJointureEqualPart(querySB, "L." + LINK_LINK_ID_COL, "T." + PERTYPE_OBJ_ID_COL);
			addAndPart(querySB);

			querySB.append(")");
			String query = getEndQuery(querySB);
			executeUpdate(query);

			// delete links in links table
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_SRC_ID_COL, srcId);

			addAndPart(querySB);
			addEqualPart(querySB, LINK_DEST_ID_COL, destId);

			query = getEndQuery(querySB);
			executeUpdate(query);

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void deleteIncomingLinks(int typeId, int destId)
			throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(typeId);
		checkDestIdParam(destId);

		deleteLinksFromObjInternal(typeId, destId, false);
	}

	private void deleteLinksFromObjInternal(int typeId, int objId,
			boolean getOutgoings) throws ModelVersionDBException {

		String objIdCol = LINK_DEST_ID_COL;
		if (getOutgoings) {
			objIdCol = LINK_SRC_ID_COL;
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
			if (!tableExist(ID_LINK_TAB) || (typeTabName == null) || !tableExist(typeId)) {
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

			addSelectPart(querySB, false, LINK_TAB + " L",
					typeTabName + "." + PERTYPE_OBJ_ID_COL);
			addWherePart(querySB);
			querySB.append(typeTabName + "." + PERTYPE_OBJ_ID_COL + " = L." + LINK_LINK_ID_COL);
			addAndPart(querySB);
			addEqualPart(querySB, "L." + objIdCol, objId);


			// );
			querySB.append(")");
			String query = getEndQuery(querySB);
			executeUpdate(query);

			// delete links in LINKS table
			querySB = getBeginDeleteQuery(LINK_TAB);
			addWherePart(querySB);
			addEqualPart(querySB, LINK_TYPE_ID_COL, typeId);
			addAndPart(querySB);
			addEqualPart(querySB, objIdCol, objId);

			query = getEndQuery(querySB);
			executeUpdate(query);

			commitInternalTransaction();
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void deleteOutgoingLinks(int typeId, int srcId)
			throws ModelVersionDBException {
		checkConnection();

		checkTypeParam(typeId);
		checkSrcIdParam(srcId);

		deleteLinksFromObjInternal(typeId, srcId, true);
	}

	public boolean isType(int objectId) throws ModelVersionDBException {
		return existIDRaw(objectId, TYPE_TAB, TYPE_TYPE_ID_COL);
	}

	public boolean hasTransaction() {
		return m_transaction;
	}

	public int createLocalIdentifier() throws ModelVersionDBException {
		return getOrCreateLocalIdentifier(UUID.randomUUID());
	}


	public Type getAttributeClass(int objectId) throws ModelVersionDBException {
		String strType =  getString(ID_ATTR_TAB, ATTR_ATTR_ID_COL, ATTR_JAVA_TYPE_COL, objectId);
		return null;
	}

	public int[] getAttributeIds(int objectId) throws ModelVersionDBException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getCommittedBy(int objectId) throws ModelVersionDBException {
		return getString(CadseGPackage.LocalId.ITEM,
				getAttributeColName(CadseGPackage.LocalId.ITEM_at_COMMITTED_BY),
				PERTYPE_OBJ_ID_COL, objectId);
	}

	public long getCommittedDate(int objectId) throws ModelVersionDBException {
		return getLong(getTypeTabName(CadseGPackage.LocalId.ITEM),
				getAttributeColName(CadseGPackage.LocalId.ITEM_at_COMMITTED_DATE),
				PERTYPE_OBJ_ID_COL, objectId, 0);
	}

	public int[] getExtendedByIds(int objectId) throws ModelVersionDBException {
		if (!isType(objectId))
			throw new IllegalArgumentException("Not a type");
		return getIds(ID_TYPE_EXT_TAB, TYPE_EXT_EXT_TYPE_ID_COL, TYPE_EXT_TYPE_ID_COL, objectId, true);
	}

	public int[] getIncomingLinks(int linkTypeId, int objectDestId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_DEST_ID_COL, objectDestId, LINK_TYPE_ID_COL, linkTypeId);
	}

	public int[] getOutgoingLinks(int linkTypeId, int objectSrcId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_SRC_ID_COL, objectSrcId, LINK_TYPE_ID_COL, linkTypeId);
	}

	public int[] getOutgoingLinkDests(int linkTypeId, int objectSrcId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_DEST_ID_COL, LINK_SRC_ID_COL, objectSrcId, LINK_TYPE_ID_COL, linkTypeId);
	}

	public int getOutgoingLinkDest(int linkTypeId, int objectSrcId) throws ModelVersionDBException {
            return getId(ID_LINK_TAB, LINK_DEST_ID_COL, LINK_SRC_ID_COL, objectSrcId, LINK_TYPE_ID_COL, linkTypeId);
	}





	private int[] getIds( int tabId, String selectCol, String whereCol1, int id1, String whereCol2, int id2) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, getTypeTabName(tabId), selectCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol1, id1);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol2, id2);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return getIntArray(rs, false);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return EMPTY_IDS;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private int[] getIds( int tabId, String selectCol,
			String whereCol1, int id1,
			String whereCol2, int id2,
			String whereCol3, int id3,
			boolean useOrder) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = null;
			if (m_connection.isHSQL() && useOrder)
				querySB = getBeginSelectQuery(true, getTypeTabName(tabId), selectCol, ORDER_COL);
			else
				querySB = getBeginSelectQuery(true, getTypeTabName(tabId), selectCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol1, id1);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol2, id2);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol3, id3);
			if (useOrder) {
				addOrderByPart(querySB, ORDER_COL);
			}
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return getIntArray(rs, false);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return EMPTY_IDS;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private int getId( int tabId, String selectCol, String whereCol1, int id1,
			String whereCol2, int id2,
			String whereCol3, int id3) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, getTypeTabName(tabId), selectCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol1, id1);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol2, id2);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol3, id3);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return rs.next()? rs.getInt(1): -1;

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return -1;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private int getId( int tabId, String selectCol, String whereCol1, int id1,
			String whereCol2, int id2) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, getTypeTabName(tabId), selectCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol1, id1);
			addAndPart(querySB);
			addEqualPart(querySB, whereCol2, id2);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return rs.next()? rs.getInt(1): -1;

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return -1;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	public int[] getIncomingLinks(int objectDestId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_DEST_ID_COL, objectDestId, false);
	}

	public int[] getLinkCompatibleVersions(int linkId) throws ModelVersionDBException {
		// TODO Auto-generated method stub
		return null;
	}

	public int[] getLinksId(int type, int srcId, int destId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_TYPE_ID_COL, type, LINK_SRC_ID_COL, srcId, LINK_DEST_ID_COL, destId, true);
	}


	public int getLinkId(int type, int srcId, int destId) throws ModelVersionDBException {
		return getId(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_TYPE_ID_COL, type, LINK_SRC_ID_COL, srcId, LINK_DEST_ID_COL, destId);
	}



	public int[] getModifiedAttributes(int objectId) throws ModelVersionDBException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName(int objectId) throws ModelVersionDBException {
		return getString(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_NAME_COL, objectId);
	}

	public int getObjectCadse(int objectId) throws ModelVersionDBException {
		return getId(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_CADSE_COL, objectId);
	}

	public void setObjectCadse(int objectId, int cadseId) throws ModelVersionDBException {
		checkObjectIdParam(objectId);
		// update attribute value in DB
		StringBuffer querySB = getBeginUpdateQuery(OBJ_TAB, new Assignment(OBJ_CADSE_COL));
		addWherePart(querySB);
		addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
		String query = getEndQuery(querySB);
		PStatement ps = null;
		try {
			beginInternalTransaction();
			ps = createPreparedStatement(query);
			ps.getStatement().setInt(1, cadseId);
			ps.executeUpdate();
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
		finally {
			close(ps);
	        }
	}

	public int getObjectIdFromQualifiedName(String qualifiedName) throws ModelVersionDBException {
		return getId(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_QNAME_COL, qualifiedName);
	}

	public int[] getObjectTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_OBJ_TYPE_TAB, OBJ_TYPE_TYPE_ID_COL, OBJ_TYPE_OBJ_ID_COL, objectId, true);
	}

	public int[] getAllObjectTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_OBJ_TYPE_TAB, ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, ALL_OBJ_TYPE_OBJ_ID_COL, objectId, false);
	}

	public DBIteratorID getAllObjectTypes2(int objectId) throws ModelVersionDBException {
		checkConnection();
		try {
			if (!tableExist(ID_ALL_OBJ_TYPE_TAB))
				return EMPTY_IDS_ITER;
			return new DBIteratorIDImpl.IterID3(this, executeQuery(
					getQueryIds(ID_ALL_OBJ_TYPE_TAB,
							ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, ALL_OBJ_TYPE_OBJ_ID_COL, objectId, false)
					));
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	public int[] getAllInstanceTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_OBJ_TYPE_TAB, ALL_OBJ_TYPE_OBJ_ID_COL, ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, objectId, false);
	}

	public int[] getAllSuperTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_TYPE_SUPER_TAB, ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, ALL_TYPE_SUPER_SUB_TYPE_ID_COL, objectId, false);
	}

	/**
	 * return all extension type ids associated to this type given by objectId parameter.
	 */
	public int[] getAllExtendesTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_EXT_TAB, ALL_EXT_EXT_TYPE_ID_COL, ALL_EXT_TYPE_ID_COL, objectId, false);
	}

	/**
	 * return all type ids extended by to this extension type given by objectId parameter.
	 */
	public int[] getAllByExtendedTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_EXT_TAB, ALL_EXT_TYPE_ID_COL, ALL_EXT_EXT_TYPE_ID_COL, objectId, false);
	}

	public int[] getAllSubTypes(int objectId) throws ModelVersionDBException {
		return getIds(ID_ALL_TYPE_SUPER_TAB, ALL_TYPE_SUPER_SUB_TYPE_ID_COL, ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, objectId, false);
	}

	public int[] getObjects(int localTypeId, int flag) throws ModelVersionDBException {
		return getIds(ID_OBJ_TYPE_TAB, OBJ_TYPE_OBJ_ID_COL, OBJ_TYPE_TYPE_ID_COL, localTypeId, false);
	}

	public int[] getOutgoingLinks(int objectSrcId) throws ModelVersionDBException {
		return getIds(ID_LINK_TAB, LINK_LINK_ID_COL, LINK_SRC_ID_COL, objectSrcId, false);
	}
    @Override
    public <T> DBIteratorID<T> getOutgoingLinks(Class<T> kind, int typeId, int objectSrcId) throws ModelVersionDBException {
         if (kind == ID3.class) {
               if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryID3(query(SELECT, ID_OBJ_CADSE_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
					AND, ID_LINK_SRC_ID_COL, EQUAL, P0,
                                        AND, ID_LINK_TYPE_ID_COL, EQUAL, P1), args(objectSrcId, typeId));
           } else if (kind == LinkInfoPlus.class) {
                if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryLinkInfoPlus(
                        /* Link T0
                         * Source Obj T1
                         * Source Type T2
                         * Dest Obj T3
                         * Dest Type T4
                         * Type obj
                         */
                        query(SELECT, T0, ID_LINK_LINK_ID_COL,
                                T6, ID_OBJ_CADSE_COL, T5, ID_OBJ_TYPE_ID_COL, T5, ID_OBJ_OBJ_ID_COL,
                                T2, ID_OBJ_CADSE_COL, T1, ID_OBJ_TYPE_ID_COL, T1, ID_OBJ_OBJ_ID_COL,
                                T4, ID_OBJ_CADSE_COL, T3, ID_OBJ_TYPE_ID_COL, T3, ID_OBJ_OBJ_ID_COL,
				FROM, ID_LINK_TAB, T0, ID_OBJ_TAB, T1, ID_OBJ_TAB, T2,
                                        ID_OBJ_TAB, T3, ID_OBJ_TAB, T4,
                                        ID_OBJ_TAB, T5, ID_OBJ_TAB, T6,
				WHERE, T0, ID_LINK_SRC_ID_COL, EQUAL, P0,
                                        AND, T0, ID_LINK_TYPE_ID_COL, EQUAL, P1,
                                        AND, T0, ID_LINK_TYPE_ID_COL, EQUAL, T5, ID_OBJ_OBJ_ID_COL,
                                        AND, T0, ID_LINK_SRC_ID_COL,  EQUAL, T1, ID_OBJ_OBJ_ID_COL,
                                        AND, T0, ID_LINK_DEST_ID_COL, EQUAL, T3, ID_OBJ_OBJ_ID_COL,
                                        AND, T5, ID_OBJ_TYPE_ID_COL,  EQUAL, T6, ID_OBJ_OBJ_ID_COL,
                                        AND, T1, ID_OBJ_TYPE_ID_COL,  EQUAL, T2, ID_OBJ_OBJ_ID_COL,
                                        AND, T3, ID_OBJ_TYPE_ID_COL,  EQUAL, T4, ID_OBJ_OBJ_ID_COL), args(objectSrcId, typeId));
           } else if (kind == LinkInfo.class) {
                if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryLinkInfo(
                        query(SELECT, ID_LINK_LINK_ID_COL, ID_LINK_TYPE_ID_COL, ID_LINK_SRC_ID_COL, ID_LINK_DEST_ID_COL,
				FROM, ID_LINK_TAB,
                                WHERE, T0, ID_LINK_TYPE_ID_COL, EQUAL, P1,
                                       AND, ID_LINK_SRC_ID_COL, EQUAL, P0), args(objectSrcId, typeId));
           }


            throw new UnsupportedOperationException("Not supported yet.");

    }

        @Override
        public <T> DBIteratorID<T> getOutgoingLinks(Class<T> kind, int objectSrcId) throws ModelVersionDBException {
            if (kind == ID3.class) {
               if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryID3(query(SELECT, ID_OBJ_CADSE_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
					AND, ID_LINK_SRC_ID_COL, EQUAL, P0), args(objectSrcId));
           } else if (kind == LinkInfoPlus.class) {
                if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryLinkInfoPlus(
                        /* Link T0
                         * Source Obj T1
                         * Source Type T2
                         * Dest Obj T3
                         * Dest Type T4
                         * Type obj
                         */
                        query(SELECT, T0, ID_LINK_LINK_ID_COL,
                                T6, ID_OBJ_CADSE_COL, T5, ID_OBJ_TYPE_ID_COL, T5, ID_OBJ_OBJ_ID_COL,
                                T2, ID_OBJ_CADSE_COL, T1, ID_OBJ_TYPE_ID_COL, T1, ID_OBJ_OBJ_ID_COL,
                                T4, ID_OBJ_CADSE_COL, T3, ID_OBJ_TYPE_ID_COL, T3, ID_OBJ_OBJ_ID_COL,
				FROM, ID_LINK_TAB, T0, ID_OBJ_TAB, T1, ID_OBJ_TAB, T2,
                                        ID_OBJ_TAB, T3, ID_OBJ_TAB, T4,
                                        ID_OBJ_TAB, T5, ID_OBJ_TAB, T6,
				WHERE, T0, ID_LINK_SRC_ID_COL, EQUAL, P0,
                                        AND, T0, ID_LINK_TYPE_ID_COL, EQUAL, T5, ID_OBJ_OBJ_ID_COL,
                                        AND, T0, ID_LINK_SRC_ID_COL,  EQUAL, T1, ID_OBJ_OBJ_ID_COL,
                                        AND, T0, ID_LINK_DEST_ID_COL, EQUAL, T3, ID_OBJ_OBJ_ID_COL,
                                        AND, T5, ID_OBJ_TYPE_ID_COL,  EQUAL, T6, ID_OBJ_OBJ_ID_COL,
                                        AND, T1, ID_OBJ_TYPE_ID_COL,  EQUAL, T2, ID_OBJ_OBJ_ID_COL,
                                        AND, T3, ID_OBJ_TYPE_ID_COL,  EQUAL, T4, ID_OBJ_OBJ_ID_COL), args(objectSrcId));
           } else if (kind == LinkInfo.class) {
                if (!tableExist(ID_LINK_TAB, ID_OBJ_TAB))
			return EMPTY_IDS_ITER;

		return (DBIteratorID<T>) executeQueryLinkInfo(
                        query(SELECT, ID_LINK_LINK_ID_COL, ID_LINK_TYPE_ID_COL, ID_LINK_LINK_ID_COL,
				FROM, ID_LINK_TAB, ID_OBJ_TAB,
				WHERE, ID_OBJ_OBJ_ID_COL, EQUAL, ID_LINK_TYPE_ID_COL,
					AND, ID_LINK_SRC_ID_COL, EQUAL, P0), args(objectSrcId));
           }


            throw new UnsupportedOperationException("Not supported yet.");
        }

	public int getParent(int objectId) throws ModelVersionDBException {
		return getId(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_PARENT_COL, objectId);
	}

	public String getQualifiedName(int objectId) throws ModelVersionDBException {
		return getString(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_QNAME_COL, objectId);
	}

	public String getAttrSqlType(int objectId) throws ModelVersionDBException {
		return getString(ID_ATTR_TAB, ATTR_ATTR_ID_COL, ATTR_SQL_TYPE_COL, objectId);
	}

	public int getAttributeSourceType(int objectId) throws ModelVersionDBException {
		return getId(ID_ATTR_TAB, ATTR_ATTR_ID_COL, ATTR_TYPE_ID_COL, objectId);
	}

	public int[] getSuperTypesIds(int objectId) throws ModelVersionDBException {
		if (!isType(objectId))
			throw new IllegalArgumentException("Not a type");
		return getIds(ID_TYPE_SUPER_TAB, TYPE_SUPER_SUPER_TYPE_ID_COL, TYPE_SUPER_SUB_TYPE_ID_COL, objectId, true);
	}

	public int[] getSubTypesIds(int objectId) throws ModelVersionDBException {
		if (!isType(objectId))
			throw new IllegalArgumentException("Not a type");
		return getIds(ID_TYPE_SUPER_TAB, TYPE_SUPER_SUB_TYPE_ID_COL, TYPE_SUPER_SUPER_TYPE_ID_COL, objectId, false);
	}

	public int[] getExtTypeIdsExtendsToThisType(int typeId)
			throws ModelVersionDBException {
		if (!isType(typeId))
			throw new IllegalArgumentException("Not a type");
		return getIds(ID_TYPE_EXT_TAB, TYPE_EXT_EXT_TYPE_ID_COL, TYPE_EXT_TYPE_ID_COL, typeId, true);
	}
	public int[] getTypeIdsExtendeByThisExt(int extId)
			throws ModelVersionDBException {
		return getIds(ID_TYPE_EXT_TAB, TYPE_EXT_TYPE_ID_COL, TYPE_EXT_EXT_TYPE_ID_COL, extId, true);
	}

	public UUID getUniqueIdentifier(int objectId) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, UUID_TAB, UUID_TAB_MSB_COL, UUID_TAB_LSB_COL);
			addWherePart(querySB);
			addEqualPart(querySB, UUID_TAB_ID_COL, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			if (!rs.next())
				return null;

			return new UUID(rs.getLong(1), rs.getLong(2));

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return null;

			throw new ModelVersionDBException(e);
		} finally {
			close (rs);
		}
	}

	public int getVersion(int objectId) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true,
					getTypeTabName(CadseGPackage.LocalId.ITEM),
					getAttributeColName(CadseGPackage.LocalId.ITEM_at_TW_VERSION));
			addWherePart(querySB);
			addEqualPart(querySB, PERTYPE_OBJ_ID_COL, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			if (!rs.next())
				return -1;

			return rs.getInt(1);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return -1;

			throw new ModelVersionDBException(e);
		} finally {
			close (rs);
		}
	}

	public boolean isAttributeDefinition(int objectId) throws ModelVersionDBException {
		return existIDRaw(objectId, ATTR_TAB, ATTR_ATTR_ID_COL);
	}

	public boolean isLink(int objectId) throws ModelVersionDBException {
		return existIDRaw(objectId, LINK_TAB, LINK_LINK_ID_COL);
	}

	private boolean existIDRaw(int objectId, String tabName, String colId) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, tabName, colId);
			addWherePart(querySB);
			addEqualPart(querySB, colId, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return rs.next();

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return false;

			throw new ModelVersionDBException(e);
		} finally {
			close (rs);
		}
	}

	public boolean isLinkType(int objectId) throws ModelVersionDBException {
		return isLink(objectId) && isAttributeDefinition(objectId);
	}

	public void saveAttributeDefinition(int attributeId, int typeId, Type javaType) throws ModelVersionDBException {
		try {
			String query;
			String sqlType = "?";
			if (javaType instanceof TypeLinkType)
				sqlType = LINK_TYPE_SQL_TYPE;
			else
				sqlType = m_connection.getTypeUtil().getSQLType(javaType);

			if (isAttributeDefinition(attributeId)) {
				int saveTypeId = getAttributeSourceType(attributeId);
				Type saveSqlType = getAttributeClass(attributeId);
			} else {

				String colName = getAttributeColName(attributeId);
				TableInfo ti = getMetadata().getOrCreateTableInfo(typeId);
				if (!(javaType instanceof TypeLinkType) && ti.created) {
						// Add a new column for attribute values
					query = getAddColumnQuery(getTypeTabName(typeId), colName ,
						sqlType);
					executeUpdate(query);
				}

				// store new attributes definition
				query = getInsertQuery(ATTR_TAB,
						new Assignment(ATTR_TYPE_ID_COL, typeId),
						new Assignment(ATTR_ATTR_ID_COL, attributeId),
						new Assignment(ATTR_SQL_TYPE_COL, sqlType));
				executeUpdate(query);

			}
		} catch (ModelVersionDBException e) {
			throw e;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	private String getAttributeColName(int attributeId) {
		return ATTR_COL_PREFIX+attributeId;
	}




	public void saveObjectPart(int objectId, int parentObject) throws ModelVersionDBException {
		if (!objExists(objectId))
			throw new IllegalArgumentException();
		if (!objExists(parentObject))
			throw new IllegalArgumentException();
		checkConnection();

		beginInternalTransaction();
		StringBuffer querySB;
		String query;
		PStatement prepare = null;
		try {
			querySB = getBeginUpdateQuery(OBJ_TAB, new Assignment(OBJ_PARENT_COL));
			addWherePart(querySB);
			addEqualPart(querySB, OBJ_OBJ_ID_COL, objectId);
			query = getEndQuery(querySB);
			prepare = createPreparedStatement(query);
			prepare.getStatement().setInt(1, parentObject);
			try {
				prepare.executeUpdate();
			} catch (IOException e) {
				throw new ModelVersionDBException(e);
			}
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (ModelVersionDBException e) {
			rollbackInternalTransaction(e);
			throw e;
		} finally {
			close(prepare);
		}
		commitInternalTransaction();
	}



	public void saveObjectVersion(int objectId, String committedBy, long committedDate, int version,
			int[] modifiedAttributes) throws ModelVersionDBException {
		// TODO Auto-generated method stub

	}

	public int[] getObjects() throws ModelVersionDBException {
		return getIds(ID_OBJ_TAB, OBJ_OBJ_ID_COL, null, -1, false);
	}

	private int[] getIds(String query, boolean useOrder) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			rs = executeQuery(query);

			return getIntArray(rs, useOrder);

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}


	private String getQueryIds(int tabId, String selectCol, String whereCol, Object id, boolean useOrder)  {
		StringBuffer querySB = getBeginSelectQuery(false, getTypeTabName(tabId), selectCol);
		if (whereCol != null) {
			addWherePart(querySB);
			addEqualPart(querySB, whereCol, id);
		}
		if (useOrder)
			addOrderByPart(querySB, ORDER_COL);
		String query = getEndQuery(querySB);
		return query;
	}

	private int[] getIds(int tabId, String selectCol, String whereCol, Object id, boolean useOrder) throws ModelVersionDBException {
		checkConnection();

		if (!tableExist(tabId))
			return EMPTY_IDS;

		return getIds(getQueryIds(tabId,selectCol, whereCol, id, useOrder), useOrder);
	}

	private int getId(int tabId, String whereCol, String returnCol, Object id) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			if (!tableExist(tabId))
				return -1;

			StringBuffer querySB = getBeginSelectQuery(false, getTypeTabName(tabId), returnCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol, id);
			String query = getEndQuery(querySB);

			rs = executeQuery(query);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return -1;

		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private String getString(int tabId, String returnCol, String whereCol, int objectId) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;

		try {
			StringBuffer querySB = getBeginSelectQuery(true, getTypeTabName(tabId), returnCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			if (!rs.next())
				return null;

			return rs.getString(1);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return null;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private long getLong(String table, String selectCol, String whereCol, int objectId, long defaultValue) throws ModelVersionDBException {
		checkConnection();
		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, table, selectCol);
			addWherePart(querySB);
			addEqualPart(querySB, whereCol, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			if (!rs.next())
				return defaultValue;

			return rs.getLong(1);

		} catch (SQLException e) {
			// table not exist means that object does not exist
			if (isTableNotExistError(e))
				return defaultValue;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	private int[] getIntArray(ResultSet rs, boolean useOrder) throws SQLException, ModelVersionDBException {
		try {
			if (rs.next()) {
				IntList ret = new IntArrayList();
				ret.add(rs.getInt(1));
				while (rs.next()) {
					ret.add(rs.getInt(1));
				}
				return ret.toArray();
			}
		} finally {
			close(rs);
		}
		return EMPTY_IDS;
	}

	public boolean uuidExists(int objectId) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			StringBuffer querySB = getBeginSelectQuery(true, UUID_TAB, UUID_TAB_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, UUID_TAB_ID_COL, objectId);
			String query = getEndQuery(querySB);

			rs = executeQuery(query, false);

			return (rs.next());

		} catch (SQLException e) {
			// table not exist is not an error
			if (isTableNotExistError(e))
				return false;

			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	void close(ResultSet rs) {
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

	public int checkLocalIdentifier(UUID uuid) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			checkTableExist(ID_UUID_TAB);

			StringBuffer querySB = getBeginSelectQuery(false, UUID_TAB, UUID_TAB_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, UUID_TAB_MSB_COL, uuid.getMostSignificantBits());
			addAndPart(querySB);
			addEqualPart(querySB, UUID_TAB_LSB_COL, uuid.getLeastSignificantBits());

			String query = getEndQuery(querySB);

			rs = executeQuery(query);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return -1;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}

	public int getOrCreateLocalIdentifier(UUID uuid) throws ModelVersionDBException {
		checkConnection();

		ResultSet rs = null;
		try {
			checkTableExist(ID_UUID_TAB);

			StringBuffer querySB = getBeginSelectQuery(false, UUID_TAB, UUID_TAB_ID_COL);
			addWherePart(querySB);
			addEqualPart(querySB, UUID_TAB_MSB_COL, uuid.getMostSignificantBits());
			addAndPart(querySB);
			addEqualPart(querySB, UUID_TAB_LSB_COL, uuid.getLeastSignificantBits());

			String query = getEndQuery(querySB);

			rs = executeQuery(query);
			if (rs.next()) {
				return rs.getInt(1);
			}
			close(rs);
			checkTableExist(ID_FREE_ID_TAB);
			beginInternalTransaction();

			query = "SELECT * FROM " + FREE_ID_TAB;
			rs = executeQueryWithScrollable(query, false);
			int localid = rs.next()? rs.getInt(1) : -1;
			close(rs);
			if (localid != -1) {
				querySB = getBeginDeleteQuery(FREE_ID_TAB);
				addWherePart(querySB);
				addEqualPart(querySB, FREE_ID_COL, localid);
				executeUpdate(getEndQuery(querySB));
			} else {
				query = "SELECT count(*) FROM " + UUID_TAB;
				rs = executeQuery(query);
				if (rs.next()) {
					localid = rs.getInt(1);
				}
				close(rs);
				while (true) {
					localid++;
					if (!uuidExists(localid))
						break;
				}
			}
			if (uuidExists(localid)) throw new ModelVersionDBException("");

			querySB = getInsertQuery(UUID_TAB, true, UUID_TAB_ID_COL, UUID_TAB_MSB_COL, UUID_TAB_LSB_COL);
			query = getEndQuery(querySB);

			PStatement ps = createPreparedStatement(query);
			ps.getStatement().setInt(1, localid);
			ps.getStatement().setLong(2, uuid.getMostSignificantBits());
			ps.getStatement().setLong(3, uuid.getLeastSignificantBits());
			ps.executeUpdate();
			ps.close();

			commitInternalTransaction();
			return localid;
		} catch (SQLException e) {
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			throw new ModelVersionDBException(e);
		} finally {
			close(rs);
		}
	}
	public void bindExtension(int typeId, int extTypeId) throws ModelVersionDBException {
		beginInternalTransaction();
		try {
			checkTableExist(ID_ALL_OBJ_TYPE_TAB);
			checkTableExist(ID_ALL_EXT_TAB);

			if  (existNupplet(TYPE_EXT_TAB, TYPE_EXT_TYPE_ID_COL, typeId,
					TYPE_EXT_EXT_TYPE_ID_COL, extTypeId))
				return ;
			insertTupleIds(TYPE_EXT_TAB, TYPE_EXT_TYPE_ID_COL, typeId,
					TYPE_EXT_EXT_TYPE_ID_COL, extTypeId);

			insertTupleIds(ALL_EXT_TAB, ALL_EXT_TYPE_ID_COL, typeId,
					ALL_EXT_EXT_TYPE_ID_COL, extTypeId);

			String query = "INSERT INTO "+	ALL_EXT_TAB+ " select " +ALL_TYPE_SUPER_SUB_TYPE_ID_COL+", "+extTypeId+
			" from "+ALL_TYPE_SUPER_TAB+ " where "+ALL_TYPE_SUPER_SUPER_TYPE_ID_COL+"="+typeId+
			" and not " +ALL_TYPE_SUPER_SUB_TYPE_ID_COL+" in (select "+ALL_EXT_TYPE_ID_COL+ " from "+
			ALL_EXT_TAB+" where "+ALL_EXT_EXT_TYPE_ID_COL+" = "+extTypeId+
			")";
			executeUpdate(query);
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public void setSuperType(int typeId, int ...superTypeIds) throws ModelVersionDBException {

		beginInternalTransaction();
		try {
			checkTableExist(ID_ALL_OBJ_TYPE_TAB);
			checkTableExist(ID_ALL_TYPE_SUPER_TAB);
			checkTableExist(ID_ALL_EXT_TAB);

			int[][] actionsIds = setIds(ID_TYPE_SUPER_TAB, TYPE_SUPER_SUB_TYPE_ID_COL, TYPE_SUPER_SUPER_TYPE_ID_COL, typeId, true, false, superTypeIds);
			int[] retainIds = actionsIds[0];
			int[] addedIds = actionsIds[1];
			int[] removedIds = actionsIds[2];

			IntBitSet allRemovedSet = new IntBitSet();
			IntBitSet allAddedSet = new IntBitSet();
		// computed all supertypes
			for (int i = 0; i < removedIds.length; i++) {
				int st = removedIds[i];
				allRemovedSet.add(st);
				int[] allst = getAllSuperTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allRemovedSet.add(allst[j]);
				}
			}
			for (int i = 0; i < addedIds.length; i++) {
				int st = addedIds[i];
				allAddedSet.add(st);
				allRemovedSet.remove(st);
				int[] allst = getAllSuperTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allRemovedSet.remove(allst[j]);
					allAddedSet.add(allst[j]);
				}
			}
			for (int i = 0; i < retainIds.length; i++) {
				int st = retainIds[i];
				allAddedSet.remove(st);
				allRemovedSet.remove(st);
				int[] allst = getAllSuperTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allRemovedSet.remove(allst[j]);
					allAddedSet.remove(allst[j]);
				}
			}
			IntIterator intIter = allAddedSet.iterator();
			while (intIter.hasNext()) {
				int id = intIter.next();
				insertTupleIds(ALL_TYPE_SUPER_TAB, ALL_TYPE_SUPER_SUB_TYPE_ID_COL, typeId,
						ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, id);
			}
			intIter = allRemovedSet.iterator();
			while (intIter.hasNext()) {
				int id = intIter.next();
				deleteTupleIds(ALL_TYPE_SUPER_TAB, ALL_TYPE_SUPER_SUB_TYPE_ID_COL, typeId,
						ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, id);
			}

			// extension added/removed
			IntBitSet allExtRemovedSet = new IntBitSet();
			IntBitSet allExtAddedSet = new IntBitSet();

			for (int i = 0; i < removedIds.length; i++) {
				int st = removedIds[i];
				int[] allst = getAllExtendesTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allExtRemovedSet.add(allst[j]);
				}
			}
			for (int i = 0; i < addedIds.length; i++) {
				int st = addedIds[i];
				int[] allst = getAllExtendesTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allExtRemovedSet.remove(allst[j]);
					allExtAddedSet.add(allst[j]);
				}
			}
			for (int i = 0; i < retainIds.length; i++) {
				int st = retainIds[i];
				int[] allst = getAllExtendesTypes(st);
				for (int j = 0; j < allst.length; j++) {
					allExtRemovedSet.remove(allst[j]);
					allExtAddedSet.remove(allst[j]);
				}
			}

			intIter = allExtAddedSet.iterator();
			while (intIter.hasNext()) {
				int id = intIter.next();
				insertTupleIds(ALL_EXT_TAB, ALL_EXT_TYPE_ID_COL, typeId,
						ALL_EXT_EXT_TYPE_ID_COL, id);
			}
			intIter = allExtRemovedSet.iterator();
			while (intIter.hasNext()) {
				int id = intIter.next();
				deleteTupleIds(ALL_EXT_TAB, ALL_EXT_TYPE_ID_COL, typeId,
						ALL_EXT_EXT_TYPE_ID_COL, id);
			}
		} catch (SQLException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		} catch (IOException e) {
			rollbackInternalTransaction(e);
			throw new ModelVersionDBException(e);
		}
	}

	public boolean isInstanceof(int instanceId, int typeId) throws ModelVersionDBException {
		return existNupplet(ALL_OBJ_TYPE_TAB, ALL_OBJ_TYPE_OBJ_ID_COL, instanceId, ALL_OBJ_TYPE_OBJ_TYPE_ID_COL, typeId);
	}

	public boolean isSuperTypeOf(int subTypeId, int superTypeId) throws ModelVersionDBException {
		return existNupplet(ALL_TYPE_SUPER_TAB,
				ALL_TYPE_SUPER_SUB_TYPE_ID_COL, subTypeId, ALL_TYPE_SUPER_SUPER_TYPE_ID_COL, superTypeId);
	}

	public boolean isExtendedBy(int typeId, int extId) throws ModelVersionDBException {
		return existNupplet(ALL_EXT_TAB,
				ALL_EXT_TYPE_ID_COL, typeId, ALL_EXT_EXT_TYPE_ID_COL, extId);
	}

	public int[] getBorderObjectIn(int cadseId) throws ModelVersionDBException {
		String query =
			"select " +LINK_DEST_ID_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " d."+OBJ_CADSE_COL+" = "+cadseId;
		return getIds(query, false);
	}

//	public String createListTable(String sqlType) throws SQLException{
//		String tableName = "LIST_"+sqlType;
//		if (tableExist(tableName))
//			return tableName;
//		StringBuffer querySB = getBeginCreateTableQuery(tableName);
//
//		addCreateTableColumnPart(querySB, ATTR_ATTR_ID_COL, m_connection.getTypeUtil().getInteger(), false);
//		addColDefSeparator(querySB);
//		addCreateTableColumnPart(querySB, OBJ_OBJ_ID_COL, m_connection.getTypeUtil().getInteger(), false);
//		addColDefSeparator(querySB);
//		addCreateTableColumnPart(querySB, ORDER_COL, m_connection.getTypeUtil().getInteger(), false);
//		addColDefSeparator(querySB);
//		addCreateTableColumnPart(querySB, "VALUE", sqlType, true);
//		addMultiplePKPart(querySB, null, true, ATTR_ATTR_ID_COL, OBJ_OBJ_ID_COL, ORDER_COL);
//
//		String query = getEndCreateTableQuery(querySB);
//
//		executeUpdate(query);
//
//		return tableName;
//	}

	/**
	 * Cadse vers qui je pointe
	 * @param cadseId
	 * @return
	 * @throws ModelVersionDBException
	 */
	public int[] getCadseOut(int cadseId) throws ModelVersionDBException {
		String query =
			"select d." +OBJ_CADSE_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " s."+OBJ_CADSE_COL+" = "+cadseId;

		return getIds(query, false);
	}

	/**
	 * Cadse qui pointe vers moi
	 * @param cadseId
	 * @return
	 * @throws ModelVersionDBException
	 */
	public int[] getCadseInt(int cadseId) throws ModelVersionDBException {
		String query =
			"select s." +OBJ_CADSE_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " d."+OBJ_CADSE_COL+" = "+cadseId;

		return getIds(query, false);
	}

	public int[] getBorderObjectOut(int cadseId) throws ModelVersionDBException {
		String query =
			"select " +LINK_SRC_ID_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " s."+OBJ_CADSE_COL+" = "+cadseId;

		return getIds(query, false);
	}

	public int[] getBorderLinksIn(int cadseId) throws ModelVersionDBException {
		String query =
			"select " +LINK_LINK_ID_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " d."+OBJ_CADSE_COL+" = "+cadseId;

		return getIds(query, false);
	}

	public int[] getBorderLinksOut(int cadseId) throws ModelVersionDBException {
		String query =
			"select " +LINK_LINK_ID_COL+" "+
			"from "+LINK_TAB+" l, "+OBJ_TAB+" s "+OBJ_TAB+ " d " +
			"where l."+LINK_SRC_ID_COL+"=s."+OBJ_OBJ_ID_COL+" and" +
				 " l."+LINK_DEST_ID_COL+"=d."+OBJ_OBJ_ID_COL+" and" +
				 " s."+OBJ_CADSE_COL+"!= d."+OBJ_CADSE_COL+" and "+
				 " s."+OBJ_CADSE_COL+" = "+cadseId;

		return getIds(query, false);
	}
	//select l.* from LINKS l, OBJECTS s, OBJECTS d
   // -> where l.SRC_ID = s.OBJ_ID and l.DEST_ID = d.OBJ_ID and s.OBJ_CADSE != d.OBJ_CADSE;

	public int[] getObjectsInCadse(int cadseId) throws ModelVersionDBException {
		return getIds(ID_OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_CADSE_COL, cadseId, false);
	}

	public boolean isBorderLink(int objectId) throws ModelVersionDBException {
		return getObjectCadse(getLinkSrc(objectId)) != getObjectCadse(getLinkDest(objectId));
	}

	private int[] ids(int... pids) {
		return pids;
	}

	public void createCadse(int cadseID, String qname, String name,
			int... extendsCadse) throws ModelVersionDBException {
		boolean cadseExist = objExists(cadseID);
		saveObject(cadseID, 0, ids(CadseGPackage.LocalId.CADSE), cadseID, qname, name, null);

		int[] cadseIds;
		if (cadseExist) {
			cadseIds = getOutgoingLinkDests(CadseGPackage.LocalId.CADSE_lt_EXTENDS, cadseID);

		} else {
			for (int c : extendsCadse)
			{
				saveLinkObject(CadseGPackage.LocalId.CADSE_lt_EXTENDS, createLocalIdentifier(), cadseID, c, null);
			}
		}
	}

	public boolean isCadse(int cadseID) throws ModelVersionDBException {
		return objExists(cadseID) && isInstanceof(cadseID, CadseGPackage.LocalId.CADSE);
	}
	public int[] findAttributesWithName(int objectId, String name) throws ModelVersionDBException {
		String query = "select DISTINCT "+ATTR_ATTR_ID_COL+" "+
				"from " +ATTR_TAB+" a, " +ALL_OBJ_TYPE_TAB+" ao, " +OBJ_TAB+" o " +
				"where " +
				          "ao." +ALL_OBJ_TYPE_OBJ_ID_COL+" ="+objectId+
				" and " + "o."  +OBJ_NAME_COL+" = " + TypeUtil.getSQLValue(name)+
				" and " + "a."  +ATTR_ATTR_ID_COL+" = o." +OBJ_OBJ_ID_COL+
				" and " + "ao." +ALL_OBJ_TYPE_OBJ_TYPE_ID_COL+" = a." +ATTR_TYPE_ID_COL;

		try {
			ResultSet rs = executeQuery(query);
			return getIntArray(rs, false); // close the resultset
		} catch(SQLException e) {
			throw new ModelVersionDBException(e);
		}
	}

	public int findAttributeWithName(int objectId, String name)
			throws ModelVersionDBException {
		int[] ret = findAttributesWithName(objectId, name);
		return ret.length == 0 ? NULL_ID : ret[0];
	}

	public void setObjectIsReadOnly(int objectId, boolean readOnly) throws ModelVersionDBException {
		setObjectValue(objectId, CadseGPackage.LocalId.ITEM_at_ITEM_READONLY, readOnly);
	}

	public void setObjectIsValid(int objectId, boolean isValid) throws ModelVersionDBException {
		setObjectValue(objectId, CadseGPackage.LocalId.ITEM_at_ISVALID, isValid);
	}

	public void setObjectVersion(int objectId, int version) throws ModelVersionDBException {
		setObjectValue(objectId, CadseGPackage.LocalId.ITEM_at_TW_VERSION, version);
	}
	public int[] getObjects(boolean b, DBExpression exp) {
		// TODO Auto-generated method stub
		return null;
	}

	Metadata getMetadata() {
		if (_metadata == null)
			_metadata = new Metadata();
		return _metadata;
	}

    public int getStatementOpen() {
//        // mysql extension...
//        return ((com.mysql.jdbc.Connection)m_connection.getConnection())
//                .getActiveStatementCount();
    	return 0;
    }

    public int load(URL url) {
        throw new UnsupportedOperationException("Not supported yet.");
    }





	@Override
	public int getObjectType(int objectId) throws ModelVersionDBException {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	public int[] getAllObjectTypesInt(int objectId)
			throws ModelVersionDBException {
		// TODO Auto-generated method stub
		return null;
	}


}
