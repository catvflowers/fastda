package com.sm.fastda.basic;

import java.util.ArrayList;
import java.util.List;

public class FastDATransaction {
	private Object conn;
	private List<Object> tasks=new ArrayList<>();
	public void addTask(Object obj) {
		if(obj==null) return;
		tasks.add(obj);
	}
	public List<Object> getTasks() {
		return tasks;
	}
	public Object getConn() {
		return conn;
	}
	public void setConn(Object conn) {
		this.conn = conn;
	}
	
	
}
