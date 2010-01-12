package fr.imag.adele.teamwork.db.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class TypeLinkType implements Type,ParameterizedType {

	
	public TypeLinkType() {
		// TODO Auto-generated constructor stub
	}
	
	public Type[] getActualTypeArguments() {
		// TODO Auto-generated method stub
		return null;
	}

	public Type getOwnerType() {
		return this;
	}

	public Type getRawType() {
		return this;
	}

}
