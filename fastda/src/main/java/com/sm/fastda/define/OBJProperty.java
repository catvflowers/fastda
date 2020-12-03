package com.sm.fastda.define;

import java.lang.reflect.Field;

public class OBJProperty {
	private String name;
	private String type;
	private String columnName;
	private String columnType;
	private OBJClass define;
	private String level;
	private Field field;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public OBJClass getDefine() {
		return define;
	}
	public void setDefine(OBJClass define) {
		this.define = define;
	}
	public Field getField() {
		return field;
	}
	public void setField(Field field) {
		this.field = field;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getLevel() {
		return level;
	}
	public void setLevel(String level) {
		this.level = level;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	
	
}
