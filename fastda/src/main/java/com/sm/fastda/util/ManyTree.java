package com.sm.fastda.util;

import java.util.Iterator;

public class ManyTree {

	private ManyTreeNode root=new ManyTreeNode();
	public ITreeNode getRootNode() {
		return this.root;
	}
	public void reset() {
		this.root=null;
	}
	/*public ITreeNode buildNode(String path,Object data) {
		ITreeNode node=null;
		if(path.equals("")) {
			node=root;
			node.setData(data);
		}else {
			String[] temp=path.split(".");
			ITreeNode parent=this.root;
			for(int i=0;i<temp.length;i++) {
				node=findNode(parent,temp[i]);
				if(node==null) {
					node=new ManyTreeNode();
					node.setName(temp[i]);
					parent.addChild(node);
					if(i==temp.length-1) node.setData(data);
					parent=node;
				}
			}
		}
		return node;
	}*/
	
	/*public ITreeNode buildNode(ITreeNode parentNode,String name,Object data) {
		ITreeNode node=new ManyTreeNode(name,data);
		parentNode.addChild(node);
		return node;
	}*/
	
	public ITreeNode findNode(ITreeNode parent,String childName) {
		Iterator<ITreeNode> it=parent.iterator();
		while(it.hasNext()) {
			ITreeNode node=it.next();
			if(node.getName().equals(childName)) return node;
		}
		return null;
	}
}
