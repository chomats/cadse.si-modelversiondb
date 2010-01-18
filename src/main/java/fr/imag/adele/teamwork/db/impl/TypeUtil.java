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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.UUID;

import fr.imag.adele.teamwork.db.ModelVersionDBException;
import fr.imag.adele.teamwork.db.ModelVersionDBService;

public class TypeUtil {
	
	/*
	 * SQL Type definition constants
	 */
	private static final String JAVA_DATE_TYPE_ANNO = "{JAVA_DATE}";
	
	private static final String UUID_TYPE_ANNO = "{UUID}";

	public static final String UNDEFINED_TYPE_ANNO = "{UNDEFINED}";

	private static final String VARCHAR_PREFIX = "VARCHAR(";
	
	private static final String VARCHAR2_PREFIX = "VARCHAR2(";
	
	private static final String CHAR_PREFIX = "CHAR(";

	/**
	 * VARCHAR length CONSTANTS
	 */
	public static final int UUID_VARCHAR_LENGTH = 36;
	
	public static final int TABLE_NAME_LENGTH = 22;
		
	public static final int STRING_VARCHAR_LENGTH = 127;
	
	/*
	 * Fields
	 */
	private String m_objectSQLType;
	
	private ConnectionDef _connect;
	
	public TypeUtil(ConnectionDef connect) {
		_connect = connect;
		
		if (connect.isMySQL())
			m_objectSQLType = SQLTypes.BLOB_SQL_TYPE_DEF;
		else if (connect.isOracle())
			m_objectSQLType = SQLTypes.BLOB_SQL_TYPE_DEF;
		else
			m_objectSQLType = SQLTypes.LONGVARBIN_SQL_TYPE_DEF;
	}
	

	public final static String getSQLValue(UUID id) {
		return getSQLValue(id.toString());
	}
	
	public final String getSQLValue(Object value) {
		if (value == null)
			return "NULL";
		
		if (value instanceof Boolean) {
			if (!_connect.isOracle())
				return value.toString();
			
			Boolean boolVal = (Boolean) value;
			if (boolVal)
				return getSQLValue("T");
			else
				return getSQLValue("F");
		}
		
		if (value instanceof java.util.Date)
			return new Long(((java.util.Date) value).getTime()).toString();
		
		if (value instanceof UUID)
			return getSQLValue((UUID) value);
		
		if (value instanceof String)
			return getSQLValue((String) value);
		
		if ((value instanceof Integer) ||
			(value instanceof Long) ||	
			(value instanceof java.sql.Time) ||
			(value instanceof java.sql.Date))
			return value.toString();
		
		if (value instanceof Serializable)
			throw new IllegalArgumentException("Cannot get sql value for serializable objects.");
		
		return value.toString();
	}
	
	public static byte[] getSerValue(Serializable value) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(value);
			oos.flush();
			
			return baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final static String getSQLValue(String str) {
		StringBuffer sb = new StringBuffer();
		sb.append("'");
		sb.append(str);
		sb.append("'");
		return sb.toString();
	}
	
	public final String getBooleanDef() {
		if (_connect.isOracle())
			return getCharDef(1);
		
		return SQLTypes.BOOLEAN_SQL_TYPE_DEF;
	}
	
	public final static String getVarcharDef(int varcharLength) {
		return VARCHAR_PREFIX + varcharLength + ")";
	}
	
	public final static String getVarchar2Def(int varcharLength) {
		return VARCHAR2_PREFIX + varcharLength + ")";
	}
	
	public final static String getCharDef(int charLength) {
		return CHAR_PREFIX + charLength + ")";
	}

	public final String getSQLType(Object value) throws ModelVersionDBException {
		if (value == null)
			return getUndefinedSQLTypeDef();
		
		if (value instanceof String) {
			int strLength = STRING_VARCHAR_LENGTH;
			int valLength = ((String) value).length();
			if (valLength > strLength)
				strLength = valLength;
			
			return getVarcharDef(strLength);
		}
		
		if (value instanceof UUID) 
			return getVarcharDef(UUID_VARCHAR_LENGTH) + UUID_TYPE_ANNO;
		
		if (value instanceof Integer || value instanceof Enum) 
			return SQLTypes.INT_SQL_TYPE_DEF;
		
		if (value instanceof Boolean) {
			if (!_connect.isOracle())
				return SQLTypes.BOOLEAN_SQL_TYPE_DEF;
			
			return getVarcharDef(1);
		}
		
		if (value instanceof Long) {
			return getLongType();
		}
		
		if (value instanceof java.util.Date) {
			if (!_connect.isOracle())
				return SQLTypes.BIGINT_SQL_TYPE_DEF + JAVA_DATE_TYPE_ANNO;
			
			return getVarchar2Def(64) + JAVA_DATE_TYPE_ANNO;
		}
		
		if (value instanceof java.sql.Time) 
			return SQLTypes.TIME_SQL_TYPE_DEF;
		
		if (value instanceof java.sql.Date) 
			return SQLTypes.DATE_SQL_TYPE_DEF;
		
		if (value instanceof Serializable) 
			return getSerSQLTypeDef();
		
		throw new ModelVersionDBException(value + " is not serializable.");
	}
	
	public final String getSQLType(Type clazz) throws ModelVersionDBException {
		if (clazz == null)
			return getUndefinedSQLTypeDef();
		
		if (clazz == String.class) {
			return getVarcharDef(STRING_VARCHAR_LENGTH);
		}
		
		if (clazz ==  UUID.class) 
			return getVarcharDef(UUID_VARCHAR_LENGTH) + UUID_TYPE_ANNO;
		
		if (clazz ==  Integer.class) 
			return SQLTypes.INT_SQL_TYPE_DEF;
		
		if (clazz ==  Boolean.class) {
			if (!_connect.isOracle())
				return SQLTypes.BOOLEAN_SQL_TYPE_DEF;
			
			return getVarcharDef(1);
		}
		
		if (clazz ==  Long.class) {
			return getLongType();
		}
		
		if (clazz ==  java.util.Date.class) {
			if (!_connect.isOracle())
				return SQLTypes.BIGINT_SQL_TYPE_DEF + JAVA_DATE_TYPE_ANNO;
			
			return getVarchar2Def(64) + JAVA_DATE_TYPE_ANNO;
		}
		
		if (clazz ==  java.sql.Time.class) 
			return SQLTypes.TIME_SQL_TYPE_DEF;
		
		if (clazz ==  java.sql.Date.class) 
			return SQLTypes.DATE_SQL_TYPE_DEF;
		if (clazz ==  Enum.class) 
			return SQLTypes.INT_SQL_TYPE_DEF;
		
		if (clazz instanceof Class && Serializable.class.isAssignableFrom((Class<?>) clazz)) 
			return getSerSQLTypeDef();
		
		throw new ModelVersionDBException(clazz + " is not serializable.");
	}


	public String getLongType() {
		if (_connect.isMySQL() || _connect.isHSQL())
			return SQLTypes.BIGINT_SQL_TYPE_DEF;
		return getVarchar2Def(64);
	}

	/**
	 * Return true if attrType cannot contain a value of newType.
	 * 
	 * @param attrType a SQL type
	 * @param newType a SQL type
	 * @return true if attrType cannot contain a value of newType.
	 */
	public boolean incompatibleTypes(String attrType, String newType) {
		if (attrType.equals(newType) || isUndefinedType(newType))
			return false;
		
		if (!attrType.startsWith(VARCHAR_PREFIX) || !newType.startsWith(VARCHAR_PREFIX))
			return true;
		
		int firstLength;
		int lastLength;
		try {
			firstLength = getVarCharLength(attrType);
			lastLength = getVarCharLength(newType);
		} catch (NumberFormatException e) {
			return true;
		}
		
		return (firstLength < lastLength);
	}

	private static int getVarCharLength(String attrType) {
		return Integer.parseInt(attrType.substring(VARCHAR_PREFIX.length(), attrType.length() - 1));
	}

	public static boolean isVarchar(String type) {
		return (type.startsWith(VARCHAR_PREFIX));
	}

	public static boolean isBlob(String type) {
		return SQLTypes.BLOB_SQL_TYPE_DEF.equals(type);
	}
	
	public static boolean isBoolean(String type) {
		return SQLTypes.BOOLEAN_SQL_TYPE_DEF.equals(type);
	}
	
	public static boolean isClob(String type) {
		return SQLTypes.CLOB_SQL_TYPE_DEF.equals(type);
	}
	
	public static boolean isObject(String type) {
		return SQLTypes.OBJ_SQL_TYPE_DEF.equals(type);
	}
	
	public static boolean isLongVarBin(String type) {
		return SQLTypes.LONGVARBIN_SQL_TYPE_DEF.equals(type);
	}
	
	public static boolean isOther(String type) {
		return SQLTypes.OTHER_SQL_TYPE_DEF.equals(type);
	}

	public static boolean isInt(String type) {
		return SQLTypes.INT_SQL_TYPE_DEF.equals(type) || "INTEGER".equals(type);
	}
	
	public String getInteger() {
		if (_connect.isHSQL())
			return "INTEGER";
		
		return SQLTypes.INT_SQL_TYPE_DEF;
	}

	public static boolean isUUID(String type) {
		return (getVarcharDef(UUID_VARCHAR_LENGTH) + UUID_TYPE_ANNO).equals(type);
	}
	
	public static boolean isJavaDate(String type) {
		return (SQLTypes.BIGINT_SQL_TYPE_DEF + JAVA_DATE_TYPE_ANNO).equals(type);
	}

	public static boolean isLong(String type) {
		return SQLTypes.BIGINT_SQL_TYPE_DEF.equals(type);
	}

	public static boolean isSQLTime(String type) {
		return SQLTypes.TIME_SQL_TYPE_DEF.equals(type);
	}

	public static boolean isSQLDate(String type) {
		return SQLTypes.DATE_SQL_TYPE_DEF.equals(type);
	}

	public boolean migratableToNewType(String attrType, String newType) {
		// We can always migrate from UNDEFINED to any type
		if (attrType.equals(newType) || isUndefinedType(attrType))
			return true;
		
		if (!attrType.startsWith(VARCHAR_PREFIX) || !newType.startsWith(VARCHAR_PREFIX) ||
				isUUID(attrType) || isUUID(newType))
			return false;
		
		int firstLength;
		int lastLength;
		try {
			firstLength = getVarCharLength(attrType);
			lastLength = getVarCharLength(newType);
		} catch (NumberFormatException e) {
			return false;
		}
		
		return (lastLength >= firstLength);
	}

	public boolean isSerType(String type) {
		return type.startsWith(getSerSQLTypeDef());
	}
	
	public String getSerSQLTypeDef() {
		return m_objectSQLType;
	}

	public boolean isUndefinedType(String type) {
		return getUndefinedSQLTypeDef().equals(type);
	}

	public static String removeTypeAnnotations(String attrType) {
		if (attrType == null)
			return null;
		
		return attrType.replaceAll("\\{.*\\}", "");
	}

	public static boolean sameBasicTypes(String type1, String type2) {
		if (type1.equals(type2))
			return true;
		
		if (type1.startsWith(VARCHAR_PREFIX) && type2.startsWith(VARCHAR_PREFIX))
			return true;
		
		return false;
	}
	
	public int getSQLtypeIntForObject() {
		if (m_objectSQLType.equals(SQLTypes.BLOB_SQL_TYPE_DEF))
			return java.sql.Types.BLOB;
		
		if (m_objectSQLType.equals(SQLTypes.LONGVARBIN_SQL_TYPE_DEF))
			return java.sql.Types.LONGVARBINARY;
		
		return java.sql.Types.BLOB;
	}
	
	private String getUndefinedSQLTypeDef() {
		return getSerSQLTypeDef() + UNDEFINED_TYPE_ANNO;
	}


	public String getSQLText() {
		if (_connect.isHSQL()) {
			return "LONGVARCHAR";
		}
		
		if (!_connect.isOracle())
			return "TEXT CHARACTER set utf8";
		
		return getVarchar2Def(1024);
	}


	

	
}
