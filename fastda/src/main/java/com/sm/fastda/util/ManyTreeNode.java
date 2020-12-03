package com.sm.fastda.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ManyTreeNode implements ITreeNode {
	private  ITreeNode parent;
	private List<ITreeNode> children=new LinkedList<ITreeNode>();
	private Object data;
	private String name;
	private int level;
	private String type;
	private String desc;
	private String alias;
	
	
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public ManyTreeNode() {
		
	}
	public ManyTreeNode(String type,String name) {
		this.type=type;
		this.name=name;
	}
	public ManyTreeNode(String type,String name, Object data) {
		this.type=type;
		this.name=name;
		this.data=data;
	}

	public void addChild(ITreeNode child) {
		child.setParent(this);
		child.setLevel(this.getLevel()+1);
		children.add(child);
	}
	
	public Iterator<ITreeNode> iterator(){
		return this.children.iterator();
	}

	public ITreeNode getParent() {
		return parent;
	}


	public int getLength() {
		return this.children.size();
	}


	public void setParent(ITreeNode parent) {
		this.parent = parent;
	}

	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public Object getData() {
		return data;
	}



	public void setData(Object data) {
		this.data = data;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}



	public int getLevel() {
		return level;
	}



	public void setLevel(int level) {
		this.level = level;
	}

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
}
