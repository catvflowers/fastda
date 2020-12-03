package com.sm.fastda.db;

import java.util.Iterator;

import com.sm.fastda.util.ITreeNode;
import com.sm.fastda.util.ManyTreeNode;

public class FastDAAST {
	 private ManyTreeNode root;
	 private ITreeNode selects;
	 private ITreeNode joins;
	 private ITreeNode wheres;
	 //private SQLTable left;
	 private String name;
	 
	public String getName() {
		return name;
	}
	public String getAlias() {
		return this.root.getData().toString();
	}
	// private ITreeNode orders;
	// private ITreeNode limits;
	 public FastDAAST(String name,String alias) {
		 this.name=name;
		 this.root=new ManyTreeNode("table",name);
		 this.root.setData(alias);
	 }
	 public ITreeNode getRootNode() {
			return this.root;
	}
	 
	public ITreeNode getSelects() {
		return selects;
	}

	public ITreeNode getJoins() {
		return joins;
	}

	public ITreeNode getWheres() {
		return wheres;
	}
	
	public void addSelect(String column){
		ITreeNode selectc=new ManyTreeNode("select",column);
		if(this.selects==null) {
			this.selects=new ManyTreeNode("selects","selects");
			this.root.addChild(this.selects);
		}
		this.selects.addChild(selectc);
	}

	public void addWhere(String column,String type,String op,String value) {
		ITreeNode wherec=new ManyTreeNode("where",column);
		wherec.setData(type+";"+op+";"+value);
		wherec.setName(column);
		if(this.wheres==null) {
			this.wheres=new ManyTreeNode("wheres","wheres");
			this.root.addChild(this.wheres);
		}
		this.wheres.addChild(wherec);
	}
	public void addJoin(String joinpath,String joinname,FastDAAST table2) {
		//find exist join
		FastDAAST old=this.findJoin(joinname);
		if(old==null) {
			ManyTreeNode join=new ManyTreeNode("join",joinname);
			join.setDesc(joinpath);
			join.setData(table2);
			if(this.joins==null) {
				this.joins=new ManyTreeNode("joins","joins");
				this.root.addChild(this.joins);
			}
			this.joins.addChild(join);
		}else {
			//merger
			//table2.
			//node.setData(data);
			merge(table2);
		}
		
	}
	public FastDAAST findJoin(String name) {
		if(this.joins==null) return null;
		Iterator<ITreeNode> it=this.joins.iterator();
		while(it.hasNext()) {
			ITreeNode node=it.next();
			String str=node.getName();
			if(str.equals(name)) return (FastDAAST)node.getData();
		}
		return null;
	}
	
	/*private ITreeNode findNode(ITreeNode parentNode,String type,boolean flag) {
		Iterator<ITreeNode> it=parentNode.iterator();
		while(it.hasNext()) {
			ITreeNode node=it.next();
			if(node.getType().equals(type)) return node;
		}
		if(flag) {
			ITreeNode node=new ManyTreeNode(type,null);
			return node;
		}
		return null;
	}*/
	/*public ITreeNode buildNode(ITreeNode parentNode,String type,Object data) {
		ITreeNode node=new ManyTreeNode(type,null,data);
		parentNode.addChild(node);
		return node;
	}*/
	
	public void merge(FastDAAST t2) {
		if(t2.joins!=null) {
			Iterator<ITreeNode> it=t2.joins.iterator();
			while(it.hasNext()) {
				ManyTreeNode node=(ManyTreeNode)it.next();
				String joinpath=node.getDesc();
				String joinname=node.getName();
				FastDAAST table2=(FastDAAST)node.getData();
				this.addJoin(joinpath,joinname, table2);
				//this.joins.addChild(node);
			}
		}
		if(t2.selects!=null) {
			Iterator<ITreeNode> it2=t2.wheres.iterator();
			while(it2.hasNext()) {
				ManyTreeNode node=(ManyTreeNode)it2.next();
				this.wheres.addChild(node);
			}
		}
		
		if(t2.selects!=null) {
			Iterator<ITreeNode> it3=t2.selects.iterator();
			while(it3.hasNext()) {
				ManyTreeNode node=(ManyTreeNode)it3.next();
				this.selects.addChild(node);
			}
		}
		
	}
}
