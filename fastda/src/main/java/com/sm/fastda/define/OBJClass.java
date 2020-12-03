package com.sm.fastda.define;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class OBJClass {
	private String sourceType;
	private String sourceName;
	private String sourceAlias;
	private String name;
	private String idName;
	private OBJClass parent;
	private Class<?> clz;
	private Map<String,OBJProperty> fields=new HashMap<>();
	private Map<String,OBJRelation> relations=new HashMap<>();
	//private Map<String,OBJCompose> composes=new HashMap<>();
	//private Map<String,OBJAggregation> aggregations=new HashMap<>();
	
	public String getSourceAlias() {
		return sourceAlias;
	}

	public Class<?> getClz() {
		return clz;
	}

	public void setClz(Class<?> clz) {
		this.clz = clz;
	}

	public void setSourceAlias(String sourceAlias) {
		this.sourceAlias = sourceAlias;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	public boolean isExtended() {
		return this.parent!=null;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public OBJClass getParent() {
		return parent;
	}
	public void setParent(OBJClass parent) {
		this.parent = parent;
	}

	public String getIdName() {
		return idName;
	}

	public void setIdName(String idName) {
		this.idName = idName;
	}

	
	
	public void  addProperty(OBJProperty objp) {
		this.fields.put(objp.getName(), objp);
	}
	
	public void  addRelation(OBJRelation objr) {
		this.relations.put(objr.getName(), objr);
	}
	
	//--------------
	public Iterator<String> iteratorProperty(){
		return this.fields.keySet().iterator();
	}
	public Iterator<String> iteratorRelation(){
		return this.relations.keySet().iterator();
	}
	public Iterator<String> iteratorAllRelation(){
		Set<String> set=new HashSet<>();
		set.addAll(this.relations.keySet());
		OBJClass cur=this;
		while(cur.parent!=null) {
			set.addAll(cur.parent.relations.keySet());
			cur=cur.parent;
		}
		return set.iterator();
	}
	public Iterator<String> iteratorAllProperty(){
		Set<String> set=new HashSet<>();
		set.addAll(this.fields.keySet());
		OBJClass cur=this;
		while(cur.parent!=null) {
			set.addAll(cur.parent.fields.keySet());
			cur=cur.parent;
		}
		return set.iterator();
	}
	public String findAllIdName() {
		if(this.parent!=null)
			return this.parent.findAllIdName();
		else
			return this.idName;
	}
	/*public String getAllName() {
		if(this.parent!=null)
			return this.parent.getName()+this.name;
		else
			return name;
	}*/
	
	public OBJProperty findProperty(String field) {
		if(this.fields.containsKey(field)) {
			return this.fields.get(field);
		}else if(this.isExtended()) {
			return this.parent.findProperty(field);
		}
		return null;
	}
	public OBJRelation findRelation(String field) {
		if(this.relations.containsKey(field)) {
			return this.relations.get(field);
		}else if(this.isExtended()) {
			return this.parent.findRelation(field);
		}
		return null;
	}
	public OBJRelation getRelation(String field) {
		if(this.relations.containsKey(field)) {
			return this.relations.get(field);
		}
		return null;
	}
	
	public OBJProperty getProperty(String field) {
		if(this.fields.containsKey(field)) {
			return this.fields.get(field);
		}
		return null;
	}
}
