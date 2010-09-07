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

/**
 *
 * @author chomats
 */
public class LoadDBInWS {
    public void readAll(LogicalWorkspace l) throws SQLException, CadseException {
		LogicalWorkspaceTransaction t = l.createTransaction();
		StringBuffer querySB = getBeginSelectQuery(false,
				OBJ_TAB, OBJ_OBJ_ID_COL, OBJ_NAME_COL, OBJ_QNAME_COL, OBJ_PARENT_COL,
				UUID_TAB_MSB_COL, UUID_TAB_LSB_COL, OBJ_CADSE_COL, OBJ_STATE_COL);
		querySB.append(", ").append(UUID_TAB);
		addWherePart(querySB);
		addJointureEqualPart(querySB, OBJ_OBJ_ID_COL, UUID_TAB_ID_COL);

		String query = getEndQuery(querySB);
		IntKeyOpenHashMap int_ItemDelta = new IntKeyOpenHashMap();;

		ResultSet rs = executeQuery(query, false);
		while (rs.next()) {
			UUID id = new UUID(rs.getLong(5), rs.getLong(6));
			ItemDelta foundItem = t.getItem(id);
			int itemState = rs.getInt(8);
			if (foundItem != null) {
				int_ItemDelta.put(rs.getInt(1), foundItem);
				if (itemState == OBJECT_STATE_UNRESOLVED && foundItem.isResolved() == false) {
					continue;
				}
				if (itemState == OBJECT_STATE_STATIC && foundItem.isStatic() == true) {
					continue;
				}
				if (itemState == OBJECT_STATE_NORMAL && foundItem.isResolved() == true) {
					continue;
				}
			}

			ItemDelta itemDelta = t.createEmptyItem(id);
			itemDelta.setObjectID(rs.getInt(1));
			itemDelta.setName(rs.getString(2), true);
			itemDelta.setQualifiedName(rs.getString(3), true);
			itemDelta.setParentID(rs.getInt(4));
			itemDelta.setCadseID(rs.getInt(7));
			int_ItemDelta.put(itemDelta.getObjectID(), itemDelta);
		}

		IntBitSet typesToLoad = new IntBitSet();

		querySB = getBeginSelectQuery(false,
				OBJ_TYPE_TAB, OBJ_TYPE_OBJ_ID_COL, OBJ_TYPE_TYPE_ID_COL,
				UUID_TAB_MSB_COL, UUID_TAB_LSB_COL);
		querySB.append(", ").append(UUID_TAB);
		addWherePart(querySB);
		addNotEqualPart(querySB, OBJ_STATE_COL, OBJECT_STATE_STATIC);
		addAndPart(querySB);
		addJointureEqualPart(querySB, OBJ_TYPE_TYPE_ID_COL, UUID_TAB_ID_COL);

		query = getEndQuery(querySB);
		rs = executeQuery(query, false);
		while (rs.next()) {
			int typeID = rs.getInt(1);
			typesToLoad.add(typeID);
			int objID = rs.getInt(2);
			ItemDelta itemDelta = (ItemDelta) int_ItemDelta.get(objID);
			if (itemDelta == null) continue;

			UUID uuidType = new UUID(rs.getLong(3), rs.getLong(4));
			ItemType it = t.getItemType(uuidType);
			if (it != null) {
				itemDelta.addItemType(it);
				continue;
			}
			ItemDelta typeDelta = t.getItem(uuidType);
			if (typeDelta == null)
				continue;
			itemDelta.addItemType(typeDelta.getAdapter(ItemType.class));
		}

		IntIterator typeIter = typesToLoad.iterator();
		while (typeIter.hasNext()) {
			int typeId = typeIter.next();
			querySB = getBeginSelectQuery(false,
					getTypeTabName(typeId));
			query = getEndQuery(querySB);
			rs = executeQuery(query, false);
			int idCol = 1;
			int nbCol = rs.getMetaData().getColumnCount();
			IAttributeType<?> att[] = new IAttributeType<?>[nbCol];
			for (int i = 0; i < att.length; i++) {
				String colName = rs.getMetaData().getColumnName(i+1);
				if (PERTYPE_OBJ_ID_COL.equals(colName)) {
					idCol = i+1;
				} else {
					int attID = Integer.parseInt(colName.substring(ATTR_COL_PREFIX.length()));
					ItemDelta itemDelta = (ItemDelta) int_ItemDelta.get(attID);
					att[i] = itemDelta.getAdapter(IAttributeType.class);
				}
			}
			while (rs.next()) {
				int objectId = rs.getInt(idCol);
				ItemDelta itemDelta = (ItemDelta) int_ItemDelta.get(objectId);
				for (int i = 0; i < att.length; i++) {
					if (i == idCol || att[i] == null) continue;
					Object v = rs.getObject(i+1);
					itemDelta.setAttribute(att[i], v);
				}
			}
		}

		querySB = getBeginSelectQuery(false,
				LINK_TAB, LINK_LINK_ID_COL, LINK_TYPE_ID_COL, LINK_SRC_ID_COL,
				LINK_DEST_ID_COL);
		query = getEndQuery(querySB);
		rs = executeQuery(query, false);
		while (rs.next()) {
			int linkId = rs.getInt(1);
			int linkTypeId = rs.getInt(2);
			int linkSrcId = rs.getInt(3);
			int linkDestId = rs.getInt(4);

			ItemDelta d = (ItemDelta) int_ItemDelta.get(linkTypeId);
			if (d == null) continue;
			LinkType lt = d.getAdapter(LinkType.class);
			if (lt == null) continue;

			ItemDelta src = (ItemDelta) int_ItemDelta.get(linkSrcId);
			if (src == null || src.isStatic()) continue;

			ItemDelta dest = (ItemDelta) int_ItemDelta.get(linkDestId);
			if (dest == null) continue;
			src.loadLink(linkId, lt, dest);
		}
		t.commit();
	}
}
