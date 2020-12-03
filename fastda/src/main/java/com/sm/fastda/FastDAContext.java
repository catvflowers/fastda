package com.sm.fastda;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sm.fastda.define.OBJClass;

public class FastDAContext {
	private static final ThreadLocal<FastDAContext> currentContext =new ThreadLocal<>();
	
	public static FastDAContext getValue() {
		FastDAContext context=currentContext.get();
		if(context==null) {
			context=new FastDAContext();
			currentContext.set(context);
		}
		return context;
	}
	public static void setValue(OBJClass define,Map<String,String> params) {
		FastDAContext context=new FastDAContext();
		context.define=define;
		context.params=params;
		currentContext.set(context);
	}
	
	public OBJClass define;
	public Map<String,String> params;
	public Map<String, Map<String, String>> groupsParams;
	public int total;
	
	public List<String> orders;
	//public String desc;
	//---------------------
	//public Object conn;
	public int deep;
	public Set<String> mustSave;
	
	public Object transaction;
	//public OBJRelation relation;
	//result
	public boolean status=false;
	//log
	public int requestId;//not reset
	public int spanId;
	private static int no1;
	private static int no2;
	private StringBuilder logs=new StringBuilder();
	public String errorMessage;
	public void log(int level,String str) {
		logs.append(level);
		logs.append(";;requestId=");
		logs.append(this.requestId);
		logs.append(";");
		logs.append(str);
		logs.append(";;");
		if(level==1) errorMessage=str;
	}
	public void log(Exception e) {
		logs.append(1);
		logs.append(";;requestId=");
		logs.append(this.requestId);
		logs.append(";");
		logs.append(e.toString()+e.getMessage());
		logs.append(";;");
		 errorMessage=e.toString()+e.getMessage();
		 e.printStackTrace();
	}
	public String getLogs() {
		return logs.toString();
	}
	public void reset() {
		this.errorMessage=null;
		this.close();
		this.mustSave=new HashSet<>();
		this.logs=new StringBuilder();
		if(this.groupsParams!=null) this.groupsParams.clear();
		if(requestId==0) {
			no1++;
			requestId=no1;
		}
		no2++;
		spanId=no2;
		
		
	}
	public void close() {
		this.define=null;
		this.params=null;
		if(this.groupsParams!=null) this.groupsParams.clear();
		this.total=-1;
		this.orders=null;
		this.deep=0;
		this.mustSave=null;
		this.transaction=null;
		this.status=false;
	}
	 
}
