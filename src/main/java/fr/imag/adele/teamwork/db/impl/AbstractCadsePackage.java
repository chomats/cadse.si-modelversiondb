package fr.imag.adele.teamwork.db.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import bak.pcj.map.IntKeyOpenHashMap;

import fr.imag.adele.teamwork.db.ModelVersionDBException;
import fr.imag.adele.teamwork.db.ModelVersionDBService2;
import fr.imag.adele.teamwork.db.TransactionException;
import fr.imag.adele.teamwork.db.impl.CadseGPackage.LocalId;

public class AbstractCadsePackage {
	final static protected boolean IS_ABSTRACT = true;
	
	final static protected boolean IS_AGGREGATION = true;
	final static protected boolean IS_ANNOTATION = true;
	final static protected boolean IS_COMPOSITION = true;
	final static protected boolean IS_PART = true;
	final static protected boolean IS_REQUIRE = true;
	final static protected boolean IS_HIDDEN = true;

	final static protected boolean ROOT_ELMENT = true;
	final static protected boolean INSTANCE_IS_HIDDEN = true;
	final static protected boolean INSTANCE_IS_READ_ONLY = true;
	
	final static protected boolean OBJECT_HIDDEN = true;
	final static protected boolean ATTRIBUT_CAN_BE_UNDEFINED = true;
	final static protected boolean ATTRIBUT_MUST_BE_INITIALIZED_AT_CREATION_TIME = true;
	final static protected boolean ATTRIBUT_TRANSIENT = true;
	final static protected boolean STRING_NOT_EMPTY = true;
	final static protected boolean IS_LIST = true;
	
	

	public ModelVersionDBImpl2 service;
	
	
	protected int[] ids(int... pids) {
		return pids;
	}
	
	public int createEINTEGER(int localCadseID, int typeId, long uuidMsb,
			long uuidLsb, String name) throws ModelVersionDBException {
		return createEAttribute(localCadseID, typeId, uuidMsb, uuidLsb, CadseGPackage.LocalId.INTEGER, name, Integer.class);
	}

	public int createEBOOLEAN(int localCadseID, int typeId, long uuidMsb,
			long uuidLsb, String name) throws ModelVersionDBException {
		return createEAttribute(localCadseID, typeId, uuidMsb, uuidLsb, CadseGPackage.LocalId.BOOLEAN, name, Boolean.class);
	}
	
	public int createEENUMERATION(int localCadseID, int typeId, long uuidMsb,
			long uuidLsb, String name) throws ModelVersionDBException {
		return createEAttribute(localCadseID, typeId, uuidMsb, uuidLsb, CadseGPackage.LocalId.ENUM, name, Enum.class);
	}
	
	public int createESTRING(int localCadseID, int typeId, long uuidMsb,
			long uuidLsb, String name) throws ModelVersionDBException {
		return createEAttribute(localCadseID, typeId, uuidMsb, uuidLsb, CadseGPackage.LocalId.STRING, name, String.class);
	}
	
	public int createEReference(int localCadseID, int typeId,
			long uuidMsb,
			long uuidLsb, String qname, String name, 
			int typeLinkTypeId, int destId) throws ModelVersionDBException {
		int id = getLocalId(uuidMsb, uuidLsb);
		service.saveObject(id, 0, ids(typeLinkTypeId), localCadseID, 
				qname, name, null);
		service.saveObjectType(id, ids(CadseGPackage.LocalId.LINK_TYPE), ids(), ids());
		service.saveAttributeDefinition(id, typeId, new TypeLinkType());
		service.saveLinkObject(typeLinkTypeId, id, typeId, destId, null);
		return id;
	}
	
	public void initEReference(int linkTypeId, int min, int max, boolean isAggregation, boolean isAnnotation, 
			boolean isComposition, boolean isPart, boolean isRequire, boolean isHidden) throws ModelVersionDBException {
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_MIN, min);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_MAX, max);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_AGGREGATION, isAggregation);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_ANNOTATION, isAnnotation);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_COMPOSITION, isComposition);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_PART, isPart);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_REQUIRE, isRequire);
		service.setObjectValue(linkTypeId, CadseGPackage.LocalId.LINK_TYPE_at_HIDDEN, isHidden);
	}
	
	public int createEAttribute(int localCadseID, int localTypeID, long uuidMsb,
			long uuidLsb, 
			int localAttrTypeID, String name, Type typeJava) throws ModelVersionDBException {
		int attID = getLocalId(uuidMsb, uuidLsb);
		String qname = service.getQualifiedName(localTypeID);
		service.saveObject(attID, 0, ids(localAttrTypeID), localCadseID, qname == null? null : qname+"."+name, name, null);
		service.saveAttributeDefinition(attID, localTypeID, typeJava);
		return attID;
	}
	
	
	public int initEClass(int typeId, boolean rootElment, 
			boolean instanceIsHidden, boolean isAbstract, 
			boolean instanceIsReadOnly, String displayName,  
			String packageName, String factoryName, String managerName,
			String icon, String qualifiedNameTemplate,
			String messageErrorName,
			String validateNameRe) throws ModelVersionDBException {
		
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_IS_ROOT_ELEMENT, rootElment);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_IS_INSTANCE_HIDDEN, instanceIsHidden);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_IS_INSTANCE_ABSTRACT, isAbstract);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_at_DISPLAY_NAME, displayName);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_ITEM_MANAGER, managerName);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_ITEM_FACTORY, factoryName);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_PACKAGE_NAME, packageName);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_ICON, icon);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_QUALIFIED_NAME_TEMPLATE, qualifiedNameTemplate);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_MESSAGE_ERROR_ID, messageErrorName);
		service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_VALIDATE_NAME_RE, validateNameRe);
		//service.setObjectValue(typeId, CadseGPackage.LocalId.ITEM_TYPE_at_INSTANCE, instanceIsReadOnly);
		return typeId;
	}
	
	public int createEClass(int cadse, long itemMsb, long itemLsb, String qname,
			String name, int[] iso, int[] superType) throws ModelVersionDBException {
		int localItemID = getLocalId(itemMsb, itemLsb);
		service.saveObject(localItemID , ModelVersionDBService2.OBJECT_STATE_STATIC, iso, cadse, qname, name, null);
		service.saveObjectType(localItemID, superType, ids(), ids());
		return localItemID;
	}

	public int createCadse(long cadsegMsb, long cadsegLsb, String qname,
			String name) throws ModelVersionDBException {
		int id = getLocalId(cadsegMsb, cadsegLsb);
		service.createCadse(id, qname, name);
		return id;
	}
	
	protected int getLocalId(long itemTypeMsb, long itemTypeLsb) throws ModelVersionDBException {
		return service.getOrCreateLocalIdentifier(new UUID(itemTypeMsb, itemTypeLsb));
	}
	
	public void createMetaLinks(List<String> cstList, IntKeyOpenHashMap cst, int objectId) throws ModelVersionDBException {
		if (service.isType(objectId)) {
			int[] superTypes = service.getSuperTypesIds(objectId);
			for (int ty : superTypes) {
				service.createLinkIfNeed(CadseGPackage.LocalId.ITEM_TYPE_lt_SUPER_TYPE, objectId, ty);
				cstList.add(cst.get(objectId)+"_subOf_"+cst.get(ty));
			}
		}
		if (service.isLinkType(objectId)) {
			int sourceId = service.getLinkSrc(objectId);
			int destId = service.getLinkDest(objectId);
			service.createLinkIfNeed(CadseGPackage.LocalId.LINK_TYPE_lt_SOURCE, objectId, sourceId);
			service.createLinkIfNeed(CadseGPackage.LocalId.LINK_TYPE_lt_DESTINATION, objectId, destId);
		}
		int[] isaTypes = service.getObjectTypes(objectId);
		for (int ty : isaTypes) {
			service.createLinkIfNeed(CadseGPackage.LocalId.ITEM_lt_INSTANCE_OF, objectId, ty);
			cstList.add(cst.get(objectId)+"_isoOf_"+cst.get(ty));
		}
	}
	
	public void createMeta(Class<?> clazz) throws ModelVersionDBException {
		IntKeyOpenHashMap cstHashMap = getCST(clazz);
		int[] ids = cstHashMap.keySet().toArray();
		List<String> cstList = new ArrayList<String>();
		for (int i = 0; i < ids.length; i++) {
			createMetaLinks(cstList , cstHashMap, ids[i]);
		}
		String[] cstArray = (String[]) cstList.toArray(new String[cstList.size()]);
		Arrays.sort(cstArray);
		for (String cst : cstArray) {
			System.out.println(cst);
		}
	}
	
	
	
	public IntKeyOpenHashMap getCST(Class<?> clazz) {
		IntKeyOpenHashMap cstHashMap = new IntKeyOpenHashMap();
		for(Field f: clazz.getFields()) {
			if (!Modifier.isStatic(f.getModifiers()))
				continue;
			try {
				int v = f.getInt(clazz);
				cstHashMap.put(v, f.getName());
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return cstHashMap;
	}
	
	public void initEAttribute(int attId, boolean objectHidden,
			boolean attributCanBeUndefined, boolean attributMustBeInitialized, boolean attributTransient, boolean isList) throws ModelVersionDBException {
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ITEM_at_ITEM_HIDDEN, objectHidden);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ATTRIBUTE_at_CANNOT_BE_UNDEFINED, attributCanBeUndefined);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ATTRIBUTE_at_MUST_BE_INITIALIZED, attributMustBeInitialized);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ATTRIBUTE_at_TRANSIENT, attributTransient);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ATTRIBUTE_at_IS_LIST, isList);
		
	}
	public void initEENUMERATION(int attId, boolean objectHidden,
			boolean attributCanBeUndefined, boolean attributMustBeInitialized, boolean attributTransient, boolean isList,
			String clazzName) throws ModelVersionDBException {
		initEAttribute(attId, objectHidden, attributCanBeUndefined, attributMustBeInitialized, attributTransient, isList);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.ENUM_at_ENUM_CLAZZ, clazzName);
	}
	public void initEINTEGER(int attId, boolean objectHidden,
			boolean attributCanBeUndefined, boolean attributMustBeInitialized, boolean attributTransient, boolean isList) throws ModelVersionDBException {
		initEAttribute(attId, objectHidden, attributCanBeUndefined, attributMustBeInitialized, attributTransient, isList);
	}
	public void initEBOOLEAN(int attId, boolean objectHidden,
			boolean attributCanBeUndefined, boolean attributMustBeInitialized, boolean attributTransient, boolean isList) throws ModelVersionDBException {
		initEAttribute(attId, objectHidden, attributCanBeUndefined, attributMustBeInitialized, attributTransient, isList);
	}
	public void initESTRING(int attId, boolean objectHidden,
			boolean attributCanBeUndefined, boolean attributMustBeInitialized, boolean attributTransient, boolean isList, boolean notEmpty) throws ModelVersionDBException {
		initEAttribute(attId, objectHidden, attributCanBeUndefined, attributMustBeInitialized, attributTransient, isList);
		this.service.setObjectValue(attId, CadseGPackage.LocalId.STRING_at_NOT_EMPTY, notEmpty);
	}
	
}
