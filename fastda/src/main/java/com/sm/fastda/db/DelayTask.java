package com.sm.fastda.db;

import java.util.List;
import java.util.Map;

import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;

public class DelayTask {
	
	public List<?> list;
	
	public OBJClass define;
	
	public Map<String,OBJProperty> props; 
	
	public String sql;
	
	//private List<DelayTask> children;
	//-------------
	//private List<Object> result;
	public List<Integer> indexs;
	//public String fkName;
	//public String joinName;
	
	public OBJClass parentDefine;
	public List<?> parentList;
	public OBJRelation relation;
	
	//private boolean dependOn;

	public boolean isDependOn() {
		return parentDefine!=null;
	}

	public void setDependOn(boolean dependOn) {
		//this.dependOn = dependOn;
		if(dependOn==false) {
			parentDefine=null;
			parentList=null;
			//fkName=null;
		}
	}
	
	

}
