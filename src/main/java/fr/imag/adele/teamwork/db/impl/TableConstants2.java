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

public class TableConstants2 {

	
	/*
	 * database table and column names
	 */

	// OBJECTS table columns
	public static final String OBJ_TAB = "OBJECTS";
	
	public static final String OBJ_OBJ_ID_COL = "OBJ_ID";

	public static final String OBJ_NAME_COL = "OBJ_NAME";
	
	public static final String OBJ_QNAME_COL = "OBJ_QNAME";
	
	public static final String OBJ_CADSE_COL = "OBJ_CADSE";
	
	public static final String OBJ_STATE_COL = "OBJ_STATE";
	
	public static final String OBJ_TYPE_ID_COL = "OBJ_TYPE_ID";
	
	public static final String OBJ_PARENT_COL = "OBJ_PARENT";
	
	
	// OBJECTS_TYPE table columns
	public static final String OBJ_TYPE_TAB = "OBJECTS_TYPE";
	
	public static final String OBJ_TYPE_OBJ_ID_COL = "OBJ_ID";

	public static final String OBJ_TYPE_TYPE_ID_COL = "OBJ_TYPE_ID";
	

	// UUID_TAB table columns
	public static final String FREE_ID_TAB = "FREE_ID";
	
	public static final String FREE_ID_COL = "OBJ_ID";
	
	// UUID_TAB table columns
	public static final String UUID_TAB = "UUID_TAB";

	public static final String UUID_TAB_ID_COL = "OBJ_ID";

	public static final String UUID_TAB_MSB_COL = "UUID_MSB";
	
	public static final String UUID_TAB_LSB_COL = "UUID_LSB";
	
	
	// TYPES table columns
	public static final String TYPE_TAB = "TYPES";

	public static final String TYPE_TYPE_ID_COL = "TYPE_ID";
	
	
	// TYPES_SUPER_EXTEN table columns
	public static final String TYPE_SUPER_TAB = "TYPES_SUPER";

	public static final String TYPE_SUPER_SUB_TYPE_ID_COL = "SUB_TYPE_ID";

	public static final String TYPE_SUPER_SUPER_TYPE_ID_COL = "SUPER_TYPE_ID";
	
	// TYPES_SUPER_EXTEN table columns
	public static final String ALL_TYPE_SUPER_TAB = "ALL_TYPES_SUPER";

	public static final String ALL_TYPE_SUPER_SUB_TYPE_ID_COL = "SUB_TYPE_ID";

	public static final String ALL_TYPE_SUPER_SUPER_TYPE_ID_COL = "SUPER_TYPE_ID";
	
	// TYPES_SUPER_EXTEN table columns
	public static final String ALL_EXT_TAB = "ALL_TYPES_EXT";

	public static final String ALL_EXT_TYPE_ID_COL = "TYPE_ID";

	public static final String ALL_EXT_EXT_TYPE_ID_COL = "EXT_TYPE_ID";
	
	// TYPES_SUPER_EXTEN table columns
	public static final String ALL_OBJ_TYPE_TAB = "ALL_OBJECTS_TYPE";

	public static final String ALL_OBJ_TYPE_OBJ_ID_COL = "OBJ_ID";

	public static final String ALL_OBJ_TYPE_OBJ_TYPE_ID_COL = "OBJ_TYPE_ID";
	
	// TYPES_SUPER_EXTEN table columns
	public static final String TYPE_EXT_TAB = "TYPES_EXT";

	public static final String TYPE_EXT_TYPE_ID_COL = "TYPE_ID";

	public static final String TYPE_EXT_EXT_TYPE_ID_COL = "EXT_TYPE_ID";
	
	
	// GEN_TABLES table columns
	public static final String GEN_TAB = "GEN_TABLES";

	public static final String GEN_TYPE_COL = "TYPE";

	public static final String GEN_LAST_IDX_COL = "LAST_IDX";
	
	// type (per object type and link type) table columns
	public static final String PERTYPE_OBJ_ID_COL = "OBJ_ID";
	
	// ATTRIBUTES table columns
	public static final String ATTR_TAB = "ATTRIBUTES";

	public static final String ATTR_TYPE_ID_COL = "TYPE_ID";

	public static final String ATTR_ATTR_ID_COL = "ATTR_ID";

	public static final String ATTR_SQL_TYPE_COL = "ATTR_TYPE";
	
	public static final String ATTR_JAVA_TYPE_COL = "ATTR_JTYPE";
	
	
	public static final String ORDER_COL = "ORDER_ELTS";

	// LINKS table columns
	public static final String LINK_TAB = "LINKS";

	public static final String LINK_LINK_ID_COL = "LINK_ID";
	
	public static final String LINK_TYPE_ID_COL = "LINK_TYPE_ID";

	public static final String LINK_SRC_ID_COL = "SRC_ID";
	
	public static final String LINK_DEST_ID_COL = "DEST_ID";
	
	
	/**
	 * Attribute column name prefix
	 */
	public static final String ATTR_COL_PREFIX = "A_";
	
	/**
	 * Savepoint name prefix
	 */
	public static final String SAVEPOINT_PREFIX = "SP_";
	

	public static final String TYPE_SAVEPOINT_NAME = "SAVEPOINT";
	
	public static final String TYPE_CLIENT_NAME = "CLIENT";
	
	/**
	 * Sequence names
	 */
	public static final String LAST_REV_SEQ_NAME = "LAST_REV_SEQ";
	
	public static final String[] TABS = { OBJ_TAB, OBJ_TYPE_TAB, FREE_ID_TAB, UUID_TAB, TYPE_TAB, 
		TYPE_SUPER_TAB, ALL_TYPE_SUPER_TAB, ALL_EXT_TAB, ALL_OBJ_TYPE_TAB,
		TYPE_EXT_TAB, GEN_TAB, ATTR_TAB, LINK_TAB };

	public static final int TAB_COUNTS = TABS.length;
	
	final static int decal_tab = 32;
	final static int decal_attr = 1024;
	
	public final static int ID_OBJ_TAB = -32;
	public final static int ID_OBJ_TYPE_TAB = -31;
	public final static int ID_FREE_ID_TAB = -30;
	public final static int ID_UUID_TAB = -29;
	public final static int ID_TYPE_TAB = -28;
	public final static int ID_TYPE_SUPER_TAB = -27;
	public final static int ID_ALL_TYPE_SUPER_TAB = -26;
	public final static int ID_ALL_EXT_TAB = -25;
	public final static int ID_ALL_OBJ_TYPE_TAB = -24;
	public final static int ID_TYPE_EXT_TAB = -23;
	public final static int ID_GEN_TAB = -22;
	public final static int ID_ATTR_TAB = -21;
	public final static int ID_LINK_TAB = -20;
	
	public final static int ID_OBJ_OBJ_ID_COL = -1024;
	public final static int ID_OBJ_NAME_COL = -1023;
	public final static int ID_OBJ_QNAME_COL = -1022;
	public final static int ID_OBJ_CADSE_COL = -1021;
	public final static int ID_OBJ_STATE_COL = -1020;
	public final static int ID_OBJ_TYPE_ID_COL = -1019;
	public final static int ID_OBJ_PARENT_COL = -1018;
	public final static int ID_OBJ_TYPE_OBJ_ID_COL = -1017;
	public final static int ID_OBJ_TYPE_TYPE_ID_COL = -1016;
	public final static int ID_FREE_ID_COL = -1015;
	public final static int ID_UUID_TAB_ID_COL = -1014;
	public final static int ID_UUID_TAB_MSB_COL = -1013;
	public final static int ID_UUID_TAB_LSB_COL = -1012;
	public final static int ID_TYPE_TYPE_ID_COL = -1011;
	public final static int ID_TYPE_SUPER_SUB_TYPE_ID_COL = -1010;
	public final static int ID_TYPE_SUPER_SUPER_TYPE_ID_COL = -1009;
	public final static int ID_ALL_TYPE_SUPER_SUB_TYPE_ID_COL = -1008;
	public final static int ID_ALL_TYPE_SUPER_SUPER_TYPE_ID_COL = -1007;
	public final static int ID_ALL_EXT_TYPE_ID_COL = -1006;
	public final static int ID_ALL_EXT_EXT_TYPE_ID_COL = -1005;
	public final static int ID_ALL_OBJ_TYPE_OBJ_ID_COL = -1004;
	public final static int ID_ALL_OBJ_TYPE_OBJ_TYPE_ID_COL = -1003;
	public final static int ID_TYPE_EXT_TYPE_ID_COL = -1002;
	public final static int ID_TYPE_EXT_EXT_TYPE_ID_COL = -1001;
	public final static int ID_GEN_TYPE_COL = -1000;
	public final static int ID_GEN_LAST_IDX_COL = -999;
	public final static int ID_PERTYPE_OBJ_ID_COL = -998;
	public final static int ID_ATTR_TYPE_ID_COL = -997;
	public final static int ID_ATTR_ATTR_ID_COL = -996;
	public final static int ID_ATTR_SQL_TYPE_COL = -995;
	public final static int ID_ATTR_JAVA_TYPE_COL = -994;
	public final static int ID_ORDER_COL = -993;
	public final static int ID_LINK_LINK_ID_COL = -992;
	public final static int ID_LINK_TYPE_ID_COL = -991;
	public final static int ID_LINK_SRC_ID_COL = -990;
	public final static int ID_LINK_DEST_ID_COL = -989;
	
	public static final String[] ATTRIBUTES_COLS_NAME = {
				OBJ_OBJ_ID_COL,
				OBJ_NAME_COL,
				OBJ_QNAME_COL,
				OBJ_CADSE_COL,
				OBJ_STATE_COL,
				OBJ_TYPE_ID_COL,
				OBJ_PARENT_COL,
				OBJ_TYPE_OBJ_ID_COL,
				OBJ_TYPE_TYPE_ID_COL,
				FREE_ID_COL,
				UUID_TAB_ID_COL,
				UUID_TAB_MSB_COL,
				UUID_TAB_LSB_COL,
				TYPE_TYPE_ID_COL,
				TYPE_SUPER_SUB_TYPE_ID_COL,
				TYPE_SUPER_SUPER_TYPE_ID_COL,
				ALL_TYPE_SUPER_SUB_TYPE_ID_COL,
				ALL_TYPE_SUPER_SUPER_TYPE_ID_COL,
				ALL_EXT_TYPE_ID_COL,
				ALL_EXT_EXT_TYPE_ID_COL,
				ALL_OBJ_TYPE_OBJ_ID_COL,
				ALL_OBJ_TYPE_OBJ_TYPE_ID_COL,
				TYPE_EXT_TYPE_ID_COL,
				TYPE_EXT_EXT_TYPE_ID_COL,
				GEN_TYPE_COL,
				GEN_LAST_IDX_COL,
				PERTYPE_OBJ_ID_COL,
				ATTR_TYPE_ID_COL,
				ATTR_ATTR_ID_COL,
				ATTR_SQL_TYPE_COL,
				ATTR_JAVA_TYPE_COL,
				ORDER_COL,
				LINK_LINK_ID_COL,
				LINK_TYPE_ID_COL,
				LINK_SRC_ID_COL,
				LINK_DEST_ID_COL
		};
}
