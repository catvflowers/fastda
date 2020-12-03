package com.sm.fastda.util;

import java.util.Iterator;

public interface ITreeNode {
	public ITreeNode getParent();
	public void setParent(ITreeNode parent);
	public Object getData();
	public void setData(Object data);
	public String getName();
	public void setName(String name);
	public int getLevel();
	public void setLevel(int level);
	public void addChild(ITreeNode child);
	public Iterator<ITreeNode> iterator();
	public int getLength();
	public String getType();
	public void setType(String type);
	
}
