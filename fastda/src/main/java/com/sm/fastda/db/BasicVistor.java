package com.sm.fastda.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sm.fastda.util.ITreeNode;
import com.sm.fastda.util.ManyTreeNode;

public class BasicVistor {
	
	public List<String> selects=new ArrayList<>();
	public List<String> tables=new ArrayList<>();
	public List<String> wheres=new ArrayList<>();
	
	//public IDAEngine engine;
	public void vistor(FastDAAST ast) {
		ITreeNode root=ast.getRootNode();
		String alias=root.getData().toString();
		String table=root.getName()+" "+alias;
		this.tables.add(table);
		if(ast.getSelects()!=null) {
			Iterator<ITreeNode> nodes=ast.getSelects().iterator();
			while(nodes.hasNext()) {
				ITreeNode node=nodes.next();
				this.selects.add(alias+"."+node.getName());
			}
		}
		if(ast.getWheres()!=null) {
			Iterator<ITreeNode> nodes=ast.getWheres().iterator();
			while(nodes.hasNext()) {
				ITreeNode node=nodes.next();
				String column=alias+"."+node.getName();
				//String[] temp=node.getData().toString().split(";");
				//String where=engine.getExp(column, temp[0], temp[1], temp[2]);
				String where=column+";"+node.getData().toString();
				this.wheres.add(where);
			}
		}
		if(ast.getJoins()!=null) {
			Iterator<ITreeNode> nodes=ast.getJoins().iterator();
			while(nodes.hasNext()) {
				ITreeNode node=nodes.next();
				ManyTreeNode mn=(ManyTreeNode)node;
				this.wheres.add(mn.getDesc());
				FastDAAST next=(FastDAAST)node.getData();
				vistor(next);
			}
		}
	}
	
	/*public String exportResult() {
		
	}*/
}
