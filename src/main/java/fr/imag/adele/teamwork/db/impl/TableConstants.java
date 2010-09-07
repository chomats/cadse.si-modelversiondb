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
 *
 * Copyright (C) 2006-2010 Adele Team/LIG/Grenoble University, France
 */
package fr.imag.adele.teamwork.db.impl;

public class TableConstants {

	/*
	 * database table and column names
	 */

	// OBJECTS table columns
	public static final String OBJ_TAB = "OBJECTS";

	public static final String OBJ_OBJ_ID_COL = "OBJ_ID";

	public static final String OBJ_TYPE_ID_COL = "OBJ_TYPE_ID";
	
	public static final String OBJ_IS_TYPE_COL = "IS_TYPE";
	
	// TYPES table columns
	public static final String TYPE_TAB = "TYPES";

	public static final String TYPE_TYPE_ID_COL = "TYPE_ID";

	public static final String TYPE_TAB_NAME_COL = "TABLE_NAME";
	
	public static final String TYPE_LINK_SRC_VERSION_SPEC_COL = "SRC_VER_SPEC";
	
	public static final String TYPE_LINK_DEST_VERSION_SPEC_COL = "DEST_VER_SPEC";
	
	// GEN_TABLES table columns
	public static final String GEN_TAB = "GEN_TABLES";

	public static final String GEN_TYPE_COL = "TYPE";

	public static final String GEN_LAST_IDX_COL = "LAST_IDX";
	
	// type (per object type and link type) table columns
	public static final String PERTYPE_OBJ_ID_COL = "OBJ_ID";
	
	public static final String PERTYPE_REV_COL = "REVISION";
	
	// ATTRIBUTES table columns
	public static final String ATTR_TAB = "ATTRIBUTES";

	public static final String ATTR_TYPE_ID_COL = "TYPE_ID";

	public static final String ATTR_ATTR_NAME_COL = "ATTRIBUTE";

	public static final String ATTR_ATTR_TYPE_COL = "ATTR_TYPE";

	public static final String ATTR_ATTR_TAB_NAME_COL = "TABLE_NAME";
	
	public static final String ATTR_VERSION_SPEC_COL = "VER_SPEC";

	// LINKS table columns
	public static final String LINK_TAB = "LINKS";

	public static final String LINK_LINK_ID_COL = "LINK_ID";
	
	public static final String LINK_REV_COL = "REVISION";

	public static final String LINK_TYPE_ID_COL = "LINK_TYPE_ID";

	public static final String LINK_SRC_ID_COL = "SRC_ID";
	
	public static final String LINK_SRC_REV_COL = "SRC_REV";

	public static final String LINK_DEST_ID_COL = "DEST_ID";
	
	public static final String LINK_DEST_REV_COL = "DEST_REV";
	
	public static final String LINK_ORDER_COL = "ORDER_NB";
	
	/**
	 * Attribute column name prefix
	 */
	public static final String ATTR_COL_PREFIX = "A_";
	
	/**
	 * Savepoint name prefix
	 */
	public static final String SAVEPOINT_PREFIX = "SP_";
	
	/**
	 * Default Entry Names
	 */
	public static final String TYPE_TYPE_NAME = "TYPE";
	
	public static final String TYPE_SAVEPOINT_NAME = "SAVEPOINT";
	
	public static final String TYPE_CLIENT_NAME = "CLIENT";
	
	/**
	 * Sequence names
	 */
	public static final String LAST_REV_SEQ_NAME = "LAST_REV_SEQ";
}
