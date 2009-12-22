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

import fr.imag.adele.teamwork.db.ModelVersionDBException;

public final class Assignment {

	private String m_column;
	private Object m_value;
	private String m_sqlType;
	private String m_sqlValue;
	private TypeUtil m_typeUtil;
	private boolean m_isSqlValue = false;
	private boolean m_isParamValue = false;
	
	public Assignment(String columnName, Object value, boolean isSqlValue, TypeUtil typeUtil) {
		m_typeUtil = typeUtil;
		m_column = columnName;
		m_isSqlValue = isSqlValue;
		if (isSqlValue) {
			m_sqlValue = (String) value;
		} else {
			m_value = value;
			initSQLType(value, typeUtil);
		}
	}
	
	public Assignment(String columnName, Object value, boolean isSqlValue) {
		this(columnName, value, isSqlValue, null);
	}

	private void initSQLType(Object value, TypeUtil typeUtil) {
		if (typeUtil == null)
			return;
		
		try {
			m_sqlType = typeUtil.getSQLType(value);
		} catch (ModelVersionDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Assignment(String columnName, Object value, TypeUtil typeUtil) {
		this(columnName, value, false, typeUtil);
	}
	
	public Assignment(String columnName, Object value) {
		this(columnName, value, false, null);
	}
	
	public Assignment(String columnName) {
		m_column = columnName;
		m_value = null;
		m_sqlValue = null;
		m_sqlType = null;
		m_isParamValue = true;
	}

	public final String getColumn() {
		return m_column;
	}
	
	public final Object getValue() {
		return m_value;
	}
	
	public final String getAssignStrValue() {
		if (m_sqlValue != null)
			return m_sqlValue;
		
		if (m_isParamValue)
			return "?";
		
		return m_typeUtil.getSQLValue(m_value);
	}

	public void setTypeUtil(TypeUtil typeUtil) {
		m_typeUtil = typeUtil;
		if (!m_isSqlValue && !m_isParamValue)
			initSQLType(m_value, typeUtil);
	}
	
}
