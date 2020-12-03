package com.sm.fastda.define;


public class OBJRelation extends OBJProperty {
	
	private String leftKey;
	private String rightKey;
	private OBJClass joinObject;
	private String leftJoin;
	private String rightJoin;
	private String name;
	private boolean master=true;
	
	private OBJClass objClass;
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public OBJClass getObjClass() {
		return objClass;
	}

	public void setObjClass(OBJClass objClass) {
		this.objClass = objClass;
	}

	public String getLeftKey() {
		return leftKey;
	}

	public void setLeftKey(String leftKey) {
		this.leftKey = leftKey;
	}

	public String getRightKey() {
		return rightKey;
	}

	public void setRightKey(String rightKey) {
		this.rightKey = rightKey;
	}

	public OBJClass getJoinObject() {
		return joinObject;
	}

	public void setJoinObject(OBJClass joinObject) {
		this.joinObject = joinObject;
	}

	public String getLeftJoin() {
		return leftJoin;
	}

	public void setLeftJoin(String leftJoin) {
		this.leftJoin = leftJoin;
	}

	public String getRightJoin() {
		return rightJoin;
	}

	public void setRightJoin(String rightJoin) {
		this.rightJoin = rightJoin;
	}

	public boolean isDirect() {
		return this.leftJoin==null;
	}

	public boolean isMaster() {
		return master;
	}

	public void setMaster(boolean master) {
		this.master = master;
	}

	
	
	
}
