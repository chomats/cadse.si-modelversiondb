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

class Attribute {

	private String m_colName;
	
	private String m_type;
	
	private boolean m_isVersionSpecific;
	
	private int typeId;
	
	Attribute(String type, String colName) {
		this(type, colName, true);
	}
	
	Attribute(String type, String colName, boolean isVersionSpec) {
		m_colName = colName;
		m_type = type;
		m_isVersionSpecific = isVersionSpec;
	}
	
	Attribute(String sqltype, String colName, int typeId) {
		m_colName = colName;
		m_type = sqltype;
		this.typeId = typeId;
	}
	
	public String getType() {
		return m_type;
	}
	
	public String getColumn() {
		return m_colName;
	}
	
	public boolean isVersionSpecific() {
		return m_isVersionSpecific;
	}

	public void setColumn(String colName) {
		m_colName = colName;
	}
	
	public int getTypeId() {
		return typeId;
	}
	
}