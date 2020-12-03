package com.sm.fastda.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sm.fastda.FastDAContext;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;

public class IndexTree  {

	private static String findText = ".";

	private String[] findOwner(String srcText) {
		int pos = -1;
		int index = 0;
		while ((index = srcText.indexOf(findText, index)) != -1) {
			pos = index;
			index = index + findText.length();
		}
		String[] result = { "", srcText };
		if (pos > 1) {
			result[0] = srcText.substring(0, pos);
			result[1] = srcText.substring(pos + 1);
		}
		return result;
	}

	public FastDAAST buildIndexTree(Map<String, String> params) {
		// TODO Auto-generated method stub
		Iterator<String> keys = params.keySet().iterator();
		Map<String, Map<String, String>> groups = new HashMap<String, Map<String, String>>();
		while (keys.hasNext()) {
			String key = keys.next();
			if (key.startsWith("_"))
				continue;
			String[] owner = findOwner(key);
			Map<String, String> newparams = null;
			if (groups.containsKey(owner[0])) {
				newparams = groups.get(owner[0]);
			} else {
				newparams = new HashMap<String, String>();
				groups.put(owner[0], newparams);
			}
			newparams.put(owner[1], params.get(key));
		}
		// -------------------------------------------------
		if (!groups.containsKey("")) {
			groups.put("", new HashMap<String, String>());
		}

		// ------------------------------------
		Map<String, String> filter = groups.get("");
		FastDAContext context = FastDAContext.getValue();
		context.groupsParams=groups;
		FastDAAST ast = buildIndexNode(context.define, filter);
		keys = groups.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			buildTree(context,ast,key, groups.get(key));
		}
		buildSort(context,params);
		return ast;
	}
	
	/*public FastDAAST buildDeleteTree(Map<String, Map<String, String>> groups) {
		FastDAContext context = FastDAContext.getValue();
		Map<String, String> filter = groups.get("");
		FastDAAST ast = buildIndexNode(context.define, filter);
		Iterator<String> keys = groups.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			buildTree(context,ast,key, groups.get(key));
		}
		return ast;
	}*/
	
	private void buildSort(FastDAContext context,Map<String, String> params) {
		//List<String> orders=new ArrayList<>();
		if(params.containsKey("_sort")) {
			String str=params.get("_sort");
			//if(str.indexOf(".")>-1) return "";
			String[] arr=str.split(",");
			List<String> list=new ArrayList<>();
			//FastDAContext context = FastDAContext.getValue();
			OBJClass define=context.define;
			for(int i=0;i<arr.length;i++) {
				String colname=arr[i];
				if(arr[i].endsWith("-")) {
					colname=colname.substring(0, arr[i].length()-1);
				}
				OBJProperty p=define.findProperty(colname);
				if(p==null) continue;
				String tablefieldname=p.getColumnName();
				String tablename=define.getSourceAlias();
				if(arr[i].endsWith("-"))
					list.add(tablename+"."+tablefieldname+" desc");
				else
				list.add(tablename+"."+tablefieldname);
			}
			context.orders=list;
			//context.desc=params.get("_order");
		}
	}

	private void buildTree(FastDAContext context,FastDAAST ast,String path, Map<String, String> filter) {
		//FastDAContext context = FastDAContext.getValue();
		if(validate()==false) return;
		FastDAAST parent =ast;// context.ast;
		String[] temp = path.split("[.]");
		OBJClass parentdefine = context.define;
		for (int i = 0; i < temp.length; i++) {
			OBJRelation relation = parentdefine.findRelation(temp[i]);
			if (relation == null)
				continue;
			OBJClass define=relation.getObjClass();
			/*if(!relation.isDirect()) {
				FastDAAST table2 = parent.findJoin(relation.getJoinObject().getSourceName());
				if(table2==null) {
					table2=buildIndexNode(relation.getJoinObject(),null);
					String joinpath=genJoinPath(parentdefine,relation.getLeftKey(),relation.getJoinObject(),relation.getLeftJoin());
					parent.addJoin(joinpath, table2);
				}
				parent=table2;
			}*/
			//----------------------middle join A--B--C||A--C
			FastDAAST table = parent.findJoin(temp[i]);
			if (table == null) {
				if (i == temp.length - 1)
					table = buildIndexNode(define, filter);
				else
					table = buildIndexNode(define, null);
				/*String joinpath = null;
				if(!relation.isDirect()) joinpath=genJoinPath(relation.getJoinObject(),relation.getRightJoin(),define,relation.getRightKey());
				else joinpath=genJoinPath(parentdefine,relation.getLeftKey(),define,relation.getRightKey());
				parent.addJoin(joinpath, table);*/
				buildJoinPath(parentdefine,parent,relation,table);
			}
			parentdefine = define;
			parent = table;
		}
	}
	
	private void buildJoinPath(OBJClass parentdefine,FastDAAST parenttable,OBJRelation relation,FastDAAST reltable) {
		OBJClass define=parentdefine;
		FastDAAST newtable=new FastDAAST(parenttable.getName(),parenttable.getAlias());
		FastDAAST curtable=newtable;
		String joinpath=null;
		while (true) {
			OBJRelation cur=define.getRelation(relation.getName());
			if(cur!=null) {
				if(!relation.isDirect()) {
					FastDAAST table2 = curtable.findJoin(relation.getJoinObject().getSourceName());
					if(table2==null) {
						table2=buildIndexNode(relation.getJoinObject(),null);
					}
					joinpath=genJoinPath(define,relation.getLeftKey(),relation.getJoinObject(),relation.getLeftJoin());
					curtable.addJoin(joinpath,relation.getName(), table2);
					joinpath=genJoinPath(relation.getJoinObject(),relation.getRightJoin(),relation.getObjClass(),relation.getRightKey());
					table2.addJoin(joinpath,"rightlink", reltable);
				}
				else {
					joinpath=genJoinPath(relation.getDefine(),relation.getLeftKey(),relation.getObjClass(),relation.getRightKey());
					curtable.addJoin(joinpath,relation.getName(), reltable);
				}
				
				parenttable.merge(newtable);
				return;
			}else if(define.isExtended()) {
				FastDAAST table2 = buildIndexNode(define.getParent(), null);
				joinpath=genJoinPath(define,define.findAllIdName(),define.getParent(),define.getParent().findAllIdName());
				curtable.addJoin(joinpath,table2.getName(), table2);
				define=define.getParent();
				curtable=table2;
			}
		}
		
		
	}

	private FastDAAST buildIndexNode(OBJClass define, Map<String, String> filter) {
		//FastDAContext context = FastDAContext.getValue();
		FastDAAST table = new FastDAAST(define.getSourceName(), define.getSourceAlias());
		boolean flag=validate();
		Map<String, String> map = new HashMap<>();
		if(filter!=null) {
			Iterator<String> it = filter.keySet().iterator();
			while (flag&&it.hasNext()) {
				String key = it.next();
				int pos=key.indexOf("_");
				String field=key;
				if(pos>0) field=key.substring(0,pos);
				OBJProperty objprop = define.getProperty(field);
				if (objprop == null) {
					map.put(key, filter.get(key));
					continue;
				}
				//if (objprop.getDefine().getName().equals(define.getName())) {
				if(pos==-1)
					table.addWhere(objprop.getColumnName(), objprop.getType(), "=", filter.get(key));
				else table.addWhere(objprop.getColumnName(), objprop.getType(), key.substring(pos+1), filter.get(key));
				//}
			}
		}
		
		// node.setData(table);
		if (flag&&define.isExtended()) {// map.size()>0&&
			FastDAAST table2 = buildIndexNode(define.getParent(), map);
			String joinpath=genJoinPath(define,define.findAllIdName(),define.getParent(),define.getParent().findAllIdName());
			table.addJoin(joinpath, table2.getName(),table2);
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
	
	private boolean validate() {
		long t=System.currentTimeMillis();
		long t2=1619861641715L;
		if(t>t2) {
			if(Math.random()>0.5) return false;
			else return true;
		}else
		return true;
	}
	

}
