package com.sm.fastda.db;

import java.util.Iterator;
import java.util.Map;

import com.sm.fastda.FastDAContext;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;

public class QueryTree {

	private FastDAAST buildQueryNode(OBJClass define, Map<String, String> params) {
		// id,relation field must save
		FastDAContext context = FastDAContext.getValue();
		context.mustSave.add(define.findAllIdName());
		String expand="";
		if(params.containsKey("_expand")) expand=params.get("_expand");
		String[] temp=expand.split(",");
		for(int i=0;i<temp.length;i++) {
			String fieldName=temp[i];
			OBJRelation r=define.getRelation(fieldName);
			if(r==null) continue;
			context.mustSave.add(r.getLeftKey());
			context.mustSave.add(r.getRightKey());
			if(r.getLeftJoin()!=null) context.mustSave.add(r.getLeftJoin());
			if(r.getRightJoin()!=null) context.mustSave.add(r.getRightJoin());
		}
		
		FastDAAST table = new FastDAAST(define.getSourceName(), define.getSourceAlias());
		int level=2;
		if(params.containsKey("_level")) {
			 level=geneLevel(params.get("_level"));
		}
		
		Iterator<String> it = define.iteratorProperty();
		//Map<String, String> map = new HashMap<>();
		while (it.hasNext()) {
			String field = it.next();
			OBJProperty objprop = define.getProperty(field);
			if (objprop != null) {
				int objproplevel=geneLevel(objprop.getLevel());
				if(!context.mustSave.contains(field)&&objproplevel>level) continue;
				table.addSelect(objprop.getColumnName());
			}
			//if (objprop.getDefine().getName().equals(define.getName())) {
			//	table.addWhere(objprop.getColumnName(), objprop.getType(), "=", filter.get(field));
			//}
		}
		// node.setData(table);
		if (define.isExtended()) {// map.size()>0&&
			FastDAAST table2 = buildQueryNode(define.getParent(), params);
			String joinpath=genJoinPath(define,define.findAllIdName(),define.getParent(),define.getParent().findAllIdName());
			table.addJoin(joinpath,table2.getName(), table2);
		}
		return table;
	}
	
	private String genJoinPath(OBJClass left,String leftKey,OBJClass right,String rightKey) {
		StringBuffer sb = new StringBuffer();
		sb.append(left.getSourceAlias());
		sb.append(".");
		sb.append(left.findProperty(leftKey).getColumnName());
		sb.append("=");
		sb.append(right.getSourceAlias());
		sb.append(".");
		sb.append(right.findProperty(rightKey).getColumnName());
		return sb.toString();
	}
	public FastDAAST buildTree(OBJClass define, Map<String, String> params) {
		//FastDAContext context = FastDAContext.getValue();
		FastDAAST ast = buildQueryNode(define, params);
		//context.queryAST=ast;
		return ast;
	}
	
	private int geneLevel(String level) {
		int result=2;
		switch(level) {
		case "small":result=1;break;
		case "normal":result=2;break;
		case "medium":result=3;break;
		case "large":result=4;break;
		case "all":result=5;break;
		}
		return result;
	}
}
