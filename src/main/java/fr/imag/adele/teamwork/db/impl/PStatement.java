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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PStatement {

	private PreparedStatement m_prepare;
	
	private List<InputStream> m_is = new ArrayList<InputStream>();
	
	public PStatement(PreparedStatement prepare) {
		m_prepare = prepare;
	}

	public PreparedStatement getStatement() {
		return m_prepare;
	}
	
	public void setBinaryStream(int idx, InputStream istream) throws SQLException {
		m_prepare.setBinaryStream(idx, istream);
		m_is.add(istream);
	}

	public void addStream(InputStream istream) {
		m_is.add(istream);
	}
	
	public void executeUpdate() throws SQLException, IOException {
		m_prepare.executeUpdate();
		for (InputStream is : m_is)
			is.close();
	}
	
	public ResultSet executeQuery() throws SQLException, IOException {
		ResultSet ret = m_prepare.executeQuery();
		for (InputStream is : m_is)
			is.close();
		return ret;
	}
	
	public void close() throws SQLException {
		m_prepare.close();
		m_prepare = null;
		m_is = null;
	}
}
