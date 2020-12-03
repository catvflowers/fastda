package com.sm.fastda.db;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import com.alibaba.fastjson.JSON;
import com.sm.fastda.FastDAContext;
import com.sm.fastda.IDAEngine;
import com.sm.fastda.basic.DataLink;
import com.sm.fastda.basic.FastDATransaction;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJCompose;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;

public class BasicRDBEngine implements IDAEngine  {
	
	/*private BasicVistor vistor;

	public void setVistor(BasicVistor vistor) {
		this.vistor = vistor;
		vistor.engine=this;
	}*/
	protected DataSource dataSource;
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	protected int _pageLimit=100;
	public List<Object> executeIndex(FastDAContext context) throws Exception  {
		Map<String,String> params=context.params;
		
		IndexTree indextree=new IndexTree();
		FastDAAST ast =indextree.buildIndexTree(params);
		
		BasicVistor vistor=new BasicVistor();
		vistor.vistor(ast);
		int _limit=_pageLimit;
		int _offset=0;
		
		
		if(params.containsKey("_limit")) _limit=Integer.parseInt(params.get("_limit").toString());
		if(params.containsKey("_page")) _offset=(Integer.parseInt(params.get("_page").toString())-1)*_limit;
		String columnId=context.define.getSourceAlias()+"."+context.define.findProperty(context.define.findAllIdName()).getColumnName();
		vistor.selects.add(columnId);
		
		boolean countflag=false;
		if(params.containsKey("_config.count")) {
			countflag=Boolean.valueOf(params.get("_config.count"));
		}
		if(countflag) {
			String sql=this.geneSelectSQL(vistor.selects,vistor.tables,vistor.wheres,null,-1,-1,1);
			int total=this.queryForCount(sql);
			FastDAContext.getValue().total=total;
		}
		String sql=this.geneSelectSQL(vistor.selects,vistor.tables,vistor.wheres,context.orders,_limit,_offset,1);
		context.log(0, "INDEX SQL="+sql);
		return this.queryForId(sql);
		
	}
	public List<?> queryOBJResult(OBJClass define,String idfield,List<Object> ids)throws Exception {
		List<?> list=queryOBJResult(define,idfield,ids,null);
		return list;
	}
	
	public List<?> queryOBJResult(OBJClass define,String idfield,List<Object> ids,String relationName)throws Exception {
		if(ids==null||ids.size()==0) return new ArrayList<Object>();
		FastDAContext context=FastDAContext.getValue();
		QueryTree tree=new QueryTree();
		FastDAAST ast=tree.buildTree(define, context.params);
		
		BasicVistor vistor=new BasicVistor();
		vistor.vistor(ast);
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<ids.size();i++) {
			sb.append(ids.get(i));
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		OBJProperty idp=define.findProperty(idfield);
		String idwhere=idp.getDefine().getSourceAlias()+"."+idp.getColumnName()+";"+idp.getType()+";in;"+sb.toString();
		vistor.wheres.add(idwhere);
		//-------------------------
		Map<String, Map<String, String>> groupsParams=context.groupsParams;
		if(relationName!=null&&groupsParams!=null&&groupsParams.containsKey(relationName)) {
			Map<String, String> params=groupsParams.get(relationName);
			Iterator<String> it=params.keySet().iterator();
			while(it.hasNext()) {
				String key=it.next();
				int pos=key.indexOf("_");
				String field=key;
				String op="=";
				if(pos>-1) {
					field=key.substring(0,pos);
					op=key.substring(pos+1);
				}
				OBJProperty property=define.findProperty(field);
				String where=property.getDefine().getSourceAlias()+"."+property.getColumnName()+";"+property.getType()+";"+op+";"+params.get(key);
				vistor.wheres.add(where);
			}
		}
		//---------------------------
		String sql=this.geneSelectSQL(vistor.selects,vistor.tables,vistor.wheres,null,-1,-1);
		context.log(0, "QUERY SQL="+sql);
		List<?> result=this.queryForList(define, sql);
		return result;
	}
	/*public void openConnection(FastDAContext context) {
		Connection conn=null;
		try {
			conn=this.dataSource.getConnection();
			context.conn=conn;
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void closeConnection(FastDAContext context) {
		try {
			if(context.conn!=null) {
				Connection conn=(Connection)context.conn;
				conn.close();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
*/
	protected List<Object> queryForId(Object obj) throws Exception{
		// TODO Auto-generated method stub
		String sqltext=obj.toString();
		List<Object> list=new ArrayList<Object>();
		//FastDAContext context = FastDAContext.getValue();
		Connection conn=null;
		try {
			conn=this.dataSource.getConnection();
			ResultSet rs=conn.createStatement().executeQuery(sqltext);
			while(rs.next()) {
				list.add(rs.getObject(1));
			}
		}finally {
			if(conn!=null)conn.close();
		}
				//(Connection)context.conn;
		
		
		return list;
	}

	protected int queryForCount(Object obj) throws SQLException  {
		// TODO Auto-generated method stub
		String sqltext="select count(*) from ("+obj.toString()+") as t";
		//System.out.println("COUNT SQL="+sqltext);
		//List<Object> list=new ArrayList<Object>();
		int count=0;
		Connection conn=null;
		try {
			conn=this.dataSource.getConnection();
			ResultSet rs=conn.createStatement().executeQuery(sqltext);
			if(rs.next())
				count=rs.getInt(1);
		}finally {
			if(conn!=null)conn.close();
		}
		return count;
	}

	protected List<?> queryForList(OBJClass objc, String sql) throws Exception {
		// TODO Auto-generated method stub
		String sqltext=sql.toString();
		//System.out.println("QUERY SQL="+sqltext);
		Connection conn=null;
		List<Object> result=new ArrayList<Object>();
		try {
			conn=this.dataSource.getConnection();
			ResultSet rs=conn.createStatement().executeQuery(sqltext);
			Class<?> clz=objc.getClz();
			Iterator<String> it=objc.iteratorAllProperty();
			Map<String,OBJProperty> map=new HashMap<>();
			while(it.hasNext()) {
				String p=it.next();
				OBJProperty objp=objc.findProperty(p);
				map.put(objp.getColumnName(), objp);
			}
			while(rs.next()) {
				Object obj=clz.newInstance();
				for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
					String tableField=rs.getMetaData().getColumnLabel(i);
					
					OBJProperty objp=map.get(tableField);
					if(objp==null) continue;
					
					Field field=objp.getField();
					String typeName=objp.getType();
					if(typeName.equals("String"))
						field.set(obj, rs.getString(tableField));
					else if(typeName.equals("Integer"))
						field.set(obj, rs.getInt(tableField));
					else if(typeName.equals("Double"))
						field.set(obj, rs.getDouble(tableField));
					else if(typeName.equals("Float"))
						field.set(obj, rs.getFloat(tableField));
					else if(typeName.equals("Date"))
						field.set(obj, rs.getDate(tableField));
					else if(typeName.equals("Time"))
						field.set(obj, rs.getTime(tableField));
					else if(typeName.equals("Timestamp"))
						field.set(obj, rs.getTimestamp(tableField));
					else if(typeName.equals("Map")||typeName.equals("JSONObject")) {
							String str=rs.getString(tableField);
							Class<?> c=field.getType();
							Object o=JSON.parseObject(str,c);
							field.set(obj, o);
					}
					else
						field.set(obj, rs.getObject(tableField));
				}
				result.add(obj);
			}
			rs.close();
			conn.close();
		}catch(Exception e) {
			throw e;
		}finally {
			if(conn!=null)try {conn.close();} catch (SQLException e) {e.printStackTrace();}
		}
		return result;
	}

	//@Override
	private String geneSelectSQL(List<String> selects,List<String> tables,List<String> wheres,List<String> orders,int limit,int offset) {
		return geneSelectSQL(selects,tables,wheres,orders,limit,offset,0);
	}
	
	protected String geneSelectSQL(List<String> selects,List<String> tables,List<String> wheres,List<String> orders,int limit,int offset,int distinct) {
		StringBuffer sb=new StringBuffer();
		sb.append("select ");
		if(distinct==1) sb.append(" distinct ");
		for(int i=0;i<selects.size();i++) {
			String fname=selects.get(i);
			if(fname.endsWith(".shape")) {
				sb.append("st_astext(");
				sb.append(fname);
				sb.append(") as shape");
			}else
				sb.append(fname);
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		
		sb.append(" from ");
		for(int i=0;i<tables.size();i++) {
			sb.append(tables.get(i));
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		
		if(wheres.size()>0) {
			sb.append(" where ");
			for(int i=0;i<wheres.size();i++) {
				String[] temp=wheres.get(i).split(";");
				String where=wheres.get(i);
				if(temp.length==4) {
					where=this.getExp(temp[0], temp[1], temp[2], temp[3]);
				}else if(temp.length==3) {
					where=this.getExp(temp[0], temp[1], temp[2],null);
				}
				//String where=this.getExp(temp[0], temp[1], temp[2], temp[3]);
				sb.append(where);
				sb.append(" and ");
			}
			sb.delete(sb.length()-5, sb.length()-1);
		}
		if(orders!=null&&orders.size()>0) {
			sb.append(geneSQLSort(orders));
		}

		if(limit>0) {
			sb.append(geneSQLLimit(limit,offset));
		}
		
		return sb.toString();
	}
	
	protected String geneSQLLimit(int limit,int offset) {
		StringBuffer sqlpage=new StringBuffer();
		sqlpage.append(" ");
		sqlpage.append("limit ");
		sqlpage.append(limit);
		sqlpage.append(" offset ");
		sqlpage.append(offset);
		
		return sqlpage.toString();
	}
	
	protected String geneSQLSort(List<String> list) {
		StringBuffer sb=new StringBuffer();
		sb.append(" order by ");
		for(int i=0;i<list.size();i++) {
			sb.append(list.get(i));
			sb.append(",");
		}
		if(sb.length()>0) sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	protected String expGeometry(String column, String type, String opChar, String value) {
		StringBuffer temp=new StringBuffer();
		if(opChar.equals("geoin")) {
			temp.append(" st_contains(st_geometryfromtext('");
			temp.append(value);
			temp.append("'),");
			temp.append(column);
			temp.append(") ");
		}else if(opChar.equals("geonear")) {
			String str=value;
			String[] arr=str.split(",");
			temp.append(" st_contains(ST_Buffer(st_geometryfromtext('");
			temp.append(arr[0]);
			temp.append("'),");
			if(arr.length==1) temp.append("100");
			else temp.append(arr[1]);
			temp.append("),");
			temp.append(column);
			temp.append(") ");
		}
		return temp.toString();
	}
	
	protected String expLikeIN(String column, String type, String opChar, String value) {
		String str=getLikeInChar(column,value);
		return str;
	}
	protected String expLike(String column, String type, String opChar, String value) {
		StringBuffer temp=new StringBuffer();
		temp.append(column);
		temp.append(" like '");
		String low=opChar.toLowerCase();
		if(low.equals("llike")) {
			temp.append(value);
			temp.append("%");
		}else if(low.equals("rlike")) {
			temp.append("%");
			temp.append(value);
		}else {
			temp.append("%");
			temp.append(value);
			temp.append("%");
		}
		temp.append("'");
		return temp.toString();
	}
	protected String expIN(String column, String type, String opChar, String value) {
		StringBuffer temp=new StringBuffer();
		temp.append(column);
		temp.append(" in (");
		temp.append(getValChar(type,value));
		temp.append(")");
		return temp.toString();
	}
	protected String expNULL(String column, String type, String opChar, String value) {
		StringBuffer temp=new StringBuffer();
		temp.append(column);
		if(opChar.equals("isnull")) temp.append(" is null");
		else temp.append(" is not null");
		return temp.toString();
	}
	protected String expBeContain(String column, String type, String opChar, String value) {
		StringBuffer temp=new StringBuffer();
		temp.append("'");
		temp.append(value);
		temp.append("' like concat(");
		temp.append(column);
		temp.append(",'%')");
		return temp.toString();
	}
	protected String expNormal(String column, String type, String opChar, String value) {
		String op=getOpChar(opChar);
		StringBuffer temp=new StringBuffer();
		temp.append(column);
		temp.append(" ");
		temp.append(op);
		temp.append(" ");
		temp.append(getValChar(type,value));
		return temp.toString();
	}

	protected String getExp(String column, String type, String opChar, String value) {
		// TODO Auto-generated method stub
		//String opChar=getOpChar(optext);
		//geo空间类型
		if(opChar.startsWith("geo")) {
			return expGeometry(column,type,opChar,value);
		}else if(opChar.endsWith("likein")) {
			return expLikeIN(column,type,opChar,value);
		}else if(opChar.endsWith("like")){
			return expLike(column,type,opChar,value);
		}else if(opChar.endsWith("becontain")){
			return expBeContain(column,type,opChar,value);
		}else if(opChar.endsWith("in")){
			return expIN(column,type,opChar,value);
		}else if(opChar.endsWith("null")){
			return expNULL(column,type,opChar,value);
		}else {
			return expNormal(column,type,opChar,value);
		}
	}
	
	private static String getOpChar(String optext) {
		if(optext.equals("gt")) return ">";
		else if(optext.equals("gte")) return ">=";
		else if(optext.equals("lt")) return "<";
		else if(optext.equals("lte")) return "<=";
		else if(optext.equals("ne")) return "!=";
		else
		return optext;
	}
	
	private static String getValChar(String simpletype,String ids) {
		if(simpletype.equals("String")||simpletype.equals("Date")||simpletype.equals("Time")||simpletype.equals("Timestamp")) {
			StringBuffer temp=new StringBuffer();
			String[] arr=ids.split(",");
			for(int i=0;i<arr.length;i++) {
				temp.append("'");
				temp.append(arr[i]);
				temp.append("',");
			}
			temp.deleteCharAt(temp.length()-1);
			return temp.toString();
		}else return ids;
	}
	
	private static String getLikeInChar(String field,String ids) {
		StringBuffer temp=new StringBuffer();
		String[] arr=ids.split(",");
		for(int i=0;i<arr.length;i++) {
			temp.append(field);
			temp.append(" like ");
			temp.append("'");
			temp.append(arr[i]);
			temp.append("%' or ");
		}
		temp.delete(temp.length()-3, temp.length());
		if(arr.length>1) {
			temp.insert(0,"(");
			temp.append(")");
		}
		return temp.toString();
	}
	//---------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------
	
	
	public <T> void insertBatch(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		if(define.isExtended()) {
			insertBatch(list,define.getParent());
		}
		//--------本地插入
		Map<String,OBJProperty> props= notNullProperty(define,list.get(0));
		if(props.size()==0) return;
		String idname=define.findAllIdName();
		OBJProperty propID=define.findProperty(idname);
		props.put(idname, propID);
		String sql=this.geneInsertSQL(define,props);
		execTask(list,define,sql,props);
		
	}
	
	public <T> void updateBatch(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		if(define.isExtended()) {
			updateBatch(list,define.getParent());
		}
		//--------本地插入
		Map<String,OBJProperty> props= notNullProperty(define,list.get(0));
		if(props.size()==0) return;
		String idname=define.findAllIdName();
		OBJProperty propID=define.findProperty(idname);
		props.put(idname, propID);
		String sql=this.geneUpdateSQL(define,props);
		//System.out.println("UPDATESQL="+sql);
		execTask(list,define,sql,props);
		//executeSaveOrUpdate(sql,list2,define,props);
	}
	
	public <T> void deleteBatch(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		if(define.isExtended()) {
			deleteBatch(list,define.getParent());
		}
		//if(props.size()==0) return;
		String sql=this.geneDeleteSQL(define);
		//System.out.println("DELETESQL="+sql);
		Map<String,OBJProperty> props= new HashMap<>();
		String idname=define.findAllIdName();
		OBJProperty propID=define.findProperty(idname);
		props.put(idname, propID);
		//DelayTask task=genTask(list,define,sql,props);
		//task.setDependOn(false);
		execTask(list,define,sql,props);
		//executeSaveOrUpdate(sql,list,define,props);
	}
	
	//----------------------------------------------------------------------------------------------------------------------
	private String geneInsertSQL(OBJClass define,Map<String,OBJProperty> props) {
		StringBuffer fieldsb=new StringBuffer();
		StringBuffer valuesb=new StringBuffer();
		Iterator<String> it=props.keySet().iterator();
		while(it.hasNext()) {
			String field=it.next();
			OBJProperty prop=props.get(field);
			fieldsb.append(prop.getColumnName());
			fieldsb.append(",");
			if(prop.getColumnType().equals("geometry"))
				valuesb.append("st_geomfromtext(?),");
			else
				valuesb.append("?,");
		}
		fieldsb.deleteCharAt(fieldsb.length()-1);
		valuesb.deleteCharAt(valuesb.length()-1);
		
		StringBuffer insertSQL=new StringBuffer();
		insertSQL.append("insert into ");
		insertSQL.append(define.getSourceName());
		insertSQL.append(" (");
		insertSQL.append(fieldsb);
		insertSQL.append(") values(");
		insertSQL.append(valuesb);
		insertSQL.append(")");
		
		return insertSQL.toString();
	}
	
	//---------------------------------------------------------------------------------------------
	
	private String geneUpdateSQL(OBJClass define,Map<String,OBJProperty> props) {
		String idname=define.findAllIdName();
		StringBuffer fieldsb=new StringBuffer();
		Iterator<String> it=props.keySet().iterator();
		while(it.hasNext()) {
			String field=it.next();
			if(idname.equals(field)) continue;
			OBJProperty prop=props.get(field);
			fieldsb.append(prop.getColumnName());
			if(prop.getColumnType().equals("geometry"))
				fieldsb.append("=st_geomfromtext(?),");
			else
				fieldsb.append("=?,");
		}
		
		fieldsb.deleteCharAt(fieldsb.length()-1);
		
		StringBuffer sql=new StringBuffer();
		sql.append("update ");
		sql.append(define.getSourceName());
		sql.append(" set ");
		sql.append(fieldsb);
		sql.append(" where ");
		sql.append(define.findProperty(idname).getColumnName());
		sql.append("=?");
		
		return sql.toString();
	}
	
	private String geneDeleteSQL(OBJClass define) {
		StringBuffer sql=new StringBuffer();
		sql.append("delete from ");
		sql.append(define.getSourceName());
		sql.append(" where ");
		sql.append(define.findProperty(define.findAllIdName()).getColumnName());
		sql.append("=?");
		
		return sql.toString();
	}
	//---------------------------------------------------------------------------
	/*private void executeSaveOrUpdate(String sql,List<?> list,OBJClass define,Map<String,OBJProperty> props) {
		executeSaveOrUpdate(sql,list,define,props,true);
	}
	protected void executeSaveOrUpdate(String sql,List<?> list,OBJClass define,Map<String,OBJProperty> props,boolean flag) {
		if(list==null) return;
		System.out.println("SQL="+sql);
		executeSaveOrUpdateAutoIncrement(sql,list,define,props,flag);
	}
	
	protected void executeSaveOrUpdateAutoIncrement(String sql,List<?> list,OBJClass define,Map<String,OBJProperty> props,boolean flag) {
		Connection conn=null;
		try {
			conn=this.dataSource.getConnection();
			conn.setAutoCommit(false);
			boolean insert=false;
			if(sql.startsWith("insert")) insert=true;
			PreparedStatement pre=null;
			if(insert)
				pre=conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
			else pre=conn.prepareStatement(sql);
			OBJProperty propID=define.findProperty(define.findAllIdName());
			int len=list.size();
			for(int i=0;i<len;i++) {
				Object obj=list.get(i);
				Object val=null;
				int pos=1;
				Iterator<String> it= props.keySet().iterator();
				while(it.hasNext()) {
					String field=it.next();
					OBJProperty prop=props.get(field);
					val=prop.getField().get(obj);
					if(val.equals("")) pre.setObject(pos, null);
					else if(prop.getColumnType().equals("json")) {
						val=JSON.toJSONString(val);
						pre.setObject(pos, val);
					}
					else pre.setObject(pos, val);
					pos++;
				}
				//set id
				if(flag)
					pre.setObject(pos, propID.getField().get(obj));
				pre.addBatch();
			}
			props.clear();
			pre.executeBatch();
			
			if(insert) {
				ResultSet rs= pre.getGeneratedKeys();
				int pos=0;
				while(rs.next()) {
					Object obj=list.get(pos);
					propID.getField().set(obj,rs.getInt(1));
					pos++;
				}
				rs.close();
			}
			
			
			pre.close();
			conn.commit();
			
		}catch(Exception e) {
			try{conn.rollback();}catch(SQLException e1) {e1.printStackTrace();}
			e.printStackTrace();
		}finally {
			if(conn!=null)try {conn.close();} catch (SQLException e) {e.printStackTrace();}
		}
	}*/
	
	private Map<String,OBJProperty> notNullProperty(OBJClass define,Object obj)  {
		Iterator<String> keys=define.iteratorProperty();
		Map<String,OBJProperty> result=new LinkedHashMap<>();
		while(keys.hasNext()) {
			String field=keys.next();
			OBJProperty prop=define.getProperty(field);
			Object val=null;
			try {
				val = prop.getField().get(obj);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(val!=null) {
				result.put(field, prop);
			}
		}
		/*FastDAContext context=FastDAContext.getValue();
		if(context.relation!=null) {
			String fkname=null;
			if(context.relation.isDirect()) fkname=context.relation.getRightKey();
			else fkname=context.relation.getLeftJoin();
			OBJProperty propfk=define.findProperty(fkname);
			if(propfk!=null) result.put(fkname, propfk);
		}*/
		//remove id
		result.remove("id");
		return result;
	}
	
	//-------------------------------------------------------------------------------
	//when insert update insert relation
	public  void addSimpleRelation(List<?> list,OBJClass define,OBJRelation relation) throws Exception {
		if(list==null||list.size()==0) return;
		if(define.isExtended()) {
			addSimpleRelation(list,define.getParent(),relation);
		}
		//--------本地插入
		if(define.getProperty(relation.getRightKey())==null) return;
		Map<String,OBJProperty> props= new LinkedHashMap<>();//notNullProperty(define,list.get(0));
		props.put(relation.getRightKey(), define.getProperty(relation.getRightKey()));
		String idname=define.findAllIdName();
		OBJProperty propID=define.findProperty(idname);
		props.put(idname, propID);
		String sql=this.geneUpdateSQL(define,props);
		//System.out.println("UPDATESQL="+sql);
		execTask(list,define,sql,props);
		//executeSaveOrUpdate(sql,list2,define,props);
	}
		public  void updateAllRelation(List<?> list,OBJClass define,OBJRelation relation) {
			try {
				if(relation instanceof OBJCompose ) {
					updateCompose(list,define,relation);
				}else if(relation.isDirect())
					updateSimpleRelation(list,define,relation);
				else updateJoin(list,define,relation);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		//----------------------------------------------------------------------------------------------------------------------
		private  void updateCompose(List<?> list,OBJClass define,OBJRelation relation) throws Exception {
			//FastDAContext context=FastDAContext.getValue();
			String idname=define.findAllIdName();
			OBJProperty fkprop2=define.findProperty(relation.getRightKey());
			OBJProperty pkprop=define.findProperty(idname);
			
			Set<Object> fkset=new HashSet<>();
			//OBJProperty fkprop1=context.parentDefine.findProperty(relation.getLeftKey());
			/*List<?> parentList=context.parentList;
			for(int i=0;i<parentList.size();i++) {
				Object obj=parentList.get(i);
				Object fkvalue=fkprop1.getField().get(obj);
				if(fkvalue==null) continue;
				fkset.add(fkvalue);
			}*/
			Set<DataLink> nowList=new HashSet<>();
			for(int i=0;i<list.size();i++) {
				Object obj=list.get(i);
				Object fkvalue=fkprop2.getField().get(obj);
				DataLink link=new DataLink();
				link.pk=pkprop.getField().get(obj);
				link.fk=fkvalue;
				nowList.add(link);
				fkset.add(fkvalue);
			}
			List<DataLink> orinalList= queryRelation(define,fkset,relation.getRightKey(),idname);
			compareDataLink(orinalList,nowList);
			//List<DataLink> sameList=compareDataLink(orinalList,nowList);
			
			List<Object> removeList=geneList(define,orinalList,relation.getRightKey(),idname);
			
			List<Object> addList=new ArrayList<>();
			for(int i=0;i<list.size();i++) {
				Object obj=list.get(i);
				Object pkobj=pkprop.getField().get(obj);
				if(pkobj==null) {
					addList.add(obj);
				}
			}
			
			//delete depondon
			this.insertBatch(addList, define);
			this.deleteBatch(removeList, define);
			//this.updateBatch(updateList, define);
		}
		private  void updateSimpleRelation(List<?> list,OBJClass define,OBJRelation relation) throws Exception {
			//FastDAContext context=FastDAContext.getValue();
			String idname=define.findAllIdName();
			OBJProperty fkprop2=define.findProperty(relation.getRightKey());
			OBJProperty pkprop=define.findProperty(idname);
			
			Set<Object> fkset=new HashSet<>();
			/*OBJProperty fkprop1=context.parentDefine.findProperty(relation.getLeftKey());
			List<?> parentList=context.parentList;
			for(int i=0;i<parentList.size();i++) {
				Object obj=parentList.get(i);
				Object fkvalue=fkprop1.getField().get(obj);
				fkset.add(fkvalue);
			}*/
			//Set<Object> fkset=context.fkset;
			Set<DataLink> nowList=new HashSet<>();
			for(int i=0;i<list.size();i++) {
				Object obj=list.get(i);
				Object pkvalue=pkprop.getField().get(obj);
				if(pkvalue==null) throw new Exception("pk is null");
				Object fkvalue=fkprop2.getField().get(obj);
				DataLink link=new DataLink();
				link.pk=pkvalue;
				link.fk=fkvalue;
				nowList.add(link);
				fkset.add(fkvalue);
			}
			List<DataLink> orinalList= queryRelation(define,fkset,relation.getRightKey(),idname);
			
			//List<DataLink> sameList=compareDataLink(orinalList,nowList);
			compareDataLink(orinalList,nowList);
			List<Object> removeList=geneList(define,orinalList,relation.getRightKey(),idname);
			List<DataLink> newList=new ArrayList<>();
			newList.addAll(nowList);
			List<Object> updateList=geneList(define,newList,relation.getRightKey(),idname);
			//updateRelation2(removeList,insertList);
			this.deleteJoinRelation(removeList, relation);
			//context.parentDefine=null;
			this.addSimpleRelation(updateList, define, relation);//remove depondon
		}
		
		private  void updateJoin(List<?> list,OBJClass define,OBJRelation relation) throws Exception {
			//FastDAContext context=FastDAContext.getValue();
			
			
			Set<Object> fkset=new HashSet<>();
			/*OBJProperty fkprop1=context.parentDefine.findProperty(relation.getLeftKey());
			List<?> parentList=context.parentList;
			for(int i=0;i<parentList.size();i++) {
				Object obj=parentList.get(i);
				Object fkvalue=fkprop1.getField().get(obj);
				fkset.add(fkvalue);
			}*/
			//Set<Object> fkset=context.fkset;
			OBJProperty fkprop1=define.findProperty(relation.getLeftJoin());
			OBJProperty fkprop2=define.findProperty(relation.getRightJoin());
			//OBJProperty pkprop=define.findProperty(define.findAllIdName());
			//List<Integer> indexs=context.indexs;
			Set<DataLink> nowList=new HashSet<>();
			for(int i=0;i<list.size();i++) {
				Object obj=list.get(i);
				Object fkvalue1=fkprop1.getField().get(obj);
				Object fkvalue2=fkprop2.getField().get(obj);
				//Object parentObj=parentList.get(indexs.get(i));
				DataLink link=new DataLink();
				link.fk=fkvalue1;
				link.pk=fkvalue2;
				nowList.add(link);
				fkset.add(fkvalue1);
			}
			OBJClass joindefine=relation.getJoinObject();
			List<DataLink> orinalList= queryRelation(joindefine,fkset,relation.getLeftJoin(),relation.getRightJoin());
			
			//List<DataLink> sameList=compareDataLink(orinalList,nowList);
			compareDataLink(orinalList,nowList);
			
			List<Object> removeList=geneList(joindefine,orinalList,relation.getLeftJoin(),relation.getRightJoin());
			List<DataLink> newList=new ArrayList<>();
			newList.addAll(nowList);
			List<Object> addList=geneList(joindefine,newList,relation.getLeftJoin(),relation.getRightJoin());
			//updateRelation2(removeList,insertList);
			this.deleteJoinRelation2(removeList, relation);
			//context.parentDefine=null;
			this.insertBatch(addList, joindefine);//remove depondon
		}
		
		
		
		private List<Object> geneList(OBJClass define,List<DataLink> list,String fkName,String pkName) throws InstantiationException, IllegalAccessException {
			if(list==null||list.size()==0) return null;
			OBJProperty fkprop=define.findProperty(fkName);
			OBJProperty pkprop=define.findProperty(pkName);
			List<Object> result=new ArrayList<>();
			Iterator<DataLink> it=list.iterator();
			while(it.hasNext()) {
				DataLink dl=it.next();
				Object obj=define.getClz().newInstance();
				pkprop.getField().set(obj, dl.pk);
				fkprop.getField().set(obj, dl.fk);
				result.add(obj);
			}
			return result;
		}
		
		private List<DataLink> compareDataLink(List<DataLink> orginal,Set<DataLink> now) {
			int len=orginal.size();
			List<DataLink> update=new ArrayList<>();
			for(int i=len-1;i>=0;i--) {
				DataLink obj1=orginal.get(i);
				if(now.contains(obj1)) {
					now.remove(obj1);
					orginal.remove(i);
					update.add(obj1);
				}
			}
			return update;
		}
		
		private  List<DataLink> queryRelation(OBJClass define,Set<Object> fkset,String fkName,String pkName) {
			//FastDAContext context=FastDAContext.getValue();
			StringBuffer ids=new StringBuffer();
			Iterator<Object> it=fkset.iterator();
			while(it.hasNext()) {
				ids.append(it.next());
				ids.append(",");
			}
			ids.deleteCharAt(ids.length()-1);
			OBJProperty fkprop=define.getProperty(fkName);
			String exp=this.getExp(fkprop.getColumnName(), fkprop.getType(), "in", ids.toString());
			
			StringBuffer sql=new StringBuffer();
			sql.append("select  ");
			sql.append(define.findProperty(pkName).getColumnName());
			sql.append(",");
			sql.append(define.findProperty(fkName).getColumnName());
			sql.append(" from ");
			sql.append(define.getSourceName());
			sql.append(" where ");
			sql.append(exp);
			
			Connection conn=null;
			List<DataLink> result=new ArrayList<>();
			try {
				conn=this.dataSource.getConnection();
				ResultSet rs=conn.createStatement().executeQuery(sql.toString());
				while(rs.next()) {
					DataLink link=new DataLink();
					link.pk=rs.getObject(1);
					link.fk=rs.getObject(2);
					result.add(link);
				}
				rs.close();
			}catch(Exception e) {
				e.printStackTrace();
			}finally {
				if(conn!=null)try {conn.close();} catch (SQLException e) {e.printStackTrace();}
			}
			return result;
		}
	//-----------------------------------------------------------------------
		public List<?> queryDeleteCompose(OBJClass define,String fkName,List<Object> idList)throws Exception {
			if(idList==null||idList.size()==0) return new ArrayList<Object>();
			//FastDAContext context=FastDAContext.getValue();
			//OBJClass parent=context.parentDefine;
			Iterator<String> it=define.iteratorAllRelation();
			Set<String> cols=new HashSet<>();
			cols.add(define.findProperty(define.findAllIdName()).getColumnName());
			while(it.hasNext()) {
				String key=it.next();
				OBJRelation relation=define.findRelation(key);
				String fkname=relation.getLeftKey();
				cols.add(define.findProperty(fkname).getColumnName());
			}
			Iterator<String> it3=cols.iterator();
			StringBuffer sb=new StringBuffer();
			sb.append("select ");
			while(it3.hasNext()) {
				sb.append(it3.next());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			sb.append(" from ");
			sb.append(define.getSourceName());
			sb.append(" where ");
			StringBuffer ids=new StringBuffer();
			Iterator<Object> it2=idList.iterator();
			while(it2.hasNext()) {
				ids.append(it2.next());
				ids.append(",");
			}
			ids.deleteCharAt(ids.length()-1);
			OBJProperty fkprop=define.findProperty(fkName);
			String exp=this.getExp(fkprop.getColumnName(), fkprop.getType(), "in", ids.toString());
			
			sb.append(exp);
			String sql=sb.toString();
			List<?> result=this.queryForList(define, sql);
			return result;
		}
		public  void deleteRelation(List<?> list,OBJRelation relation) throws Exception {
				if(relation.isDirect()) {
					this.deleteSimpleRelation(list, relation);
				}else {
					this.deleteJoinRelation(list, relation);
				}
		}
		private  void deleteJoinRelation(List<?> list,OBJRelation relation) throws Exception {
			if(list==null||list.size()==0) return;
			OBJClass define=relation.getJoinObject();
			OBJProperty fkprop=define.findProperty(relation.getLeftJoin());
			
			StringBuffer sql=new StringBuffer();
			sql.append("delete from   ");
			sql.append(define.getSourceName());
			sql.append(" where ");
			sql.append(fkprop.getColumnName());
			sql.append("=?");
			
			List<Object> result=new ArrayList<>();
			for(int i=0;i<list.size();i++) {
				Object obj=define.getClz().newInstance();
				fkprop.getField().set(obj, list.get(i));
				result.add(obj);
			}
			Map<String,OBJProperty> props=new HashMap<>();
			props.put(relation.getRightKey(), fkprop);
			execTask(result, define, sql.toString(), props);
			//DelayTask task=this.genTask(result, define, sql.toString(), props);
			//task.setDependOn(false);
		}	
		
		private  void deleteJoinRelation2(List<?> list,OBJRelation relation) throws Exception {
			if(list==null||list.size()==0) return;
			OBJClass define=relation.getJoinObject();
			OBJProperty leftprop=define.findProperty(relation.getLeftJoin());
			OBJProperty rightprop=define.findProperty(relation.getRightJoin());
			StringBuffer sql=new StringBuffer();
			sql.append("delete from   ");
			sql.append(define.getSourceName());
			sql.append(" where ");
			sql.append(leftprop.getColumnName());
			sql.append("=? and ");
			sql.append(rightprop.getColumnName());
			sql.append("=?  ");
			
			//List<?> result=new ArrayList<>();
			
			Map<String,OBJProperty> props=new LinkedHashMap<>();
			props.put(relation.getLeftJoin(), leftprop);
			props.put(relation.getRightJoin(), rightprop);
			//DelayTask task=this.genTask(list, define, sql.toString(), props);
			//task.setDependOn(false);
			execTask(list, define, sql.toString(), props);
		}	
		
		private  void deleteSimpleRelation(List<?> list,OBJRelation relation) throws Exception {
			if(list==null||list.size()==0) return;
			OBJClass define=relation.getObjClass();
			OBJProperty fkprop=define.findProperty(relation.getRightKey());
			
			StringBuffer sql=new StringBuffer();
			sql.append("update   ");
			sql.append(define.getSourceName());
			sql.append(" set  ");
			sql.append(fkprop.getColumnName());
			sql.append("=null");
			sql.append(" where ");
			sql.append(fkprop.getColumnName());
			sql.append("=?");
			
			List<Object> result=new ArrayList<>();
			for(int i=0;i<list.size();i++) {
				Object obj=define.getClz().newInstance();
				fkprop.getField().set(obj, list.get(i));
				result.add(obj);
			}
			Map<String,OBJProperty> props=new HashMap<>();
			props.put(relation.getRightKey(), fkprop);
			//DelayTask task=this.genTask(result, define, sql.toString(), props);
			//task.setDependOn(false);
			execTask(result, define, sql.toString(), props);
		}	
		//-----------------------------------
		public void startTransaction(FastDATransaction ts) {
			Connection conn=null;
			try {
				conn=this.dataSource.getConnection();
				conn.setAutoCommit(false);
				ts.setConn(conn);
			}catch(Exception e) {
				try{conn.rollback();}catch(SQLException e1) {e1.printStackTrace();}
				e.printStackTrace();
			}
			
		}
		public void commitTransaction(FastDATransaction ts) {
			/*List<Object> tasks=ts.getTasks();
			if(tasks.size()==0) return;
			Connection conn=null;
			try {
				conn=this.dataSource.getConnection();
				conn.setAutoCommit(false);
				for(int i=0;i<tasks.size();i++) {
					DelayTask task=(DelayTask)tasks.get(i);
					execTask(conn,task);
				}
				conn.commit();
			}catch(Exception e) {
				try{conn.rollback();}catch(SQLException e1) {e1.printStackTrace();}
				e.printStackTrace();
			}finally {
				if(conn!=null)try {conn.close();} catch (SQLException e) {e.printStackTrace();}
			}*/
			Connection conn=(Connection)ts.getConn();
			try {
				conn.commit();
			}catch(Exception e) {
				try{conn.rollback();}catch(SQLException e1) {e1.printStackTrace();}
				e.printStackTrace();
			}finally {
				if(conn!=null)try {conn.close();} catch (SQLException e) {e.printStackTrace();}
			}
			
		}
		
		private void execTask(List<?> list,OBJClass define,String sql,Map<String,OBJProperty> props) throws Exception {
			boolean insert=false;
			if(list==null) return;
			FastDAContext context=FastDAContext.getValue();
			if(sql.startsWith("insert")) insert=true;
			PreparedStatement pre=null;
			context.log(0, "SQL="+sql);
			//System.out.println("SQL="+sql);
			Connection conn=(Connection)((FastDATransaction)FastDAContext.getValue().transaction).getConn();
			if(insert&&define.isExtended()==false)
				pre=conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
			else pre=conn.prepareStatement(sql);
			
			int len=list.size();
			for(int i=0;i<len;i++) {
				Object obj=list.get(i);
				Object val=null;
				int pos=1;
				Iterator<String> it= props.keySet().iterator();
				StringBuilder sb=new StringBuilder();
				while(it.hasNext()) {
					String field=it.next();
					OBJProperty prop=props.get(field);
					val=prop.getField().get(obj);
					if(val==null||val.equals("")) pre.setObject(pos, null);
					else if(prop.getColumnType().equals("json")) {
						val=JSON.toJSONString(val);
						pre.setObject(pos, val);
					}
					else pre.setObject(pos, val);
					//System.out.print(val);
					//System.out.print(",");
					sb.append(val);
					sb.append(",");
					pos++;
				}
				//System.out.print("||");
				sb.append("||");
				context.log(0, sb.toString());
				pre.addBatch();
			}
			System.out.println("");
			props.clear();
			pre.executeBatch();
			
			if(insert&&define.isExtended()==false) {
				ResultSet rs= pre.getGeneratedKeys();
				int pos=0;
				OBJProperty propID=define.findProperty(define.findAllIdName());
				while(rs.next()) {
					Object obj=list.get(pos);
					propID.getField().set(obj,rs.getInt(1));
					pos++;
				}
				rs.close();
			}
			pre.close();
		}
		
		/*private void execTask(Connection conn,DelayTask task) throws SQLException, IllegalArgumentException, IllegalAccessException {
			boolean insert=false;
			String sql=task.sql;
			List<?> list=task.list;
			if(list==null) return;
			OBJClass define=task.define;
			Map<String,OBJProperty> props=task.props;
			if(sql.startsWith("insert")) insert=true;
			PreparedStatement pre=null;
			System.out.println("SQL="+sql);
			if(insert)
				pre=conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
			else pre=conn.prepareStatement(sql);
			
			int len=list.size();
			for(int i=0;i<len;i++) {
				Object obj=list.get(i);
				Object val=null;
				int pos=1;
				Iterator<String> it= props.keySet().iterator();
				while(it.hasNext()) {
					String field=it.next();
					OBJProperty prop=props.get(field);
					if(task.isDependOn()) {
						if(task.relation.isDirect()&&field.equals(task.relation.getRightKey())) {
							Object parentObj=task.parentList.get(task.indexs.get(i));
							OBJProperty propFK=task.parentDefine.findProperty(task.relation.getLeftKey());
							val=propFK.getField().get(parentObj);
						}else if(!task.relation.isDirect()&&field.equals(task.relation.getLeftJoin())) {
							Object parentObj=task.parentList.get(task.indexs.get(i));
							OBJProperty propFK=task.parentDefine.findProperty(task.relation.getLeftKey());
							val=propFK.getField().get(parentObj);
						}else val=prop.getField().get(obj);
								
					}else
						val=prop.getField().get(obj);
					
					if(val==null||val.equals("")) pre.setObject(pos, null);
					else if(prop.getColumnType().equals("json")) {
						val=JSON.toJSONString(val);
						pre.setObject(pos, val);
					}
					else pre.setObject(pos, val);
					System.out.print(val);
					System.out.print(",");
					pos++;
				}
				System.out.print("||");
				pre.addBatch();
			}
			System.out.println("");
			props.clear();
			pre.executeBatch();
			
			if(insert) {
				ResultSet rs= pre.getGeneratedKeys();
				int pos=0;
				OBJProperty propID=define.findProperty(define.findAllIdName());
				while(rs.next()) {
					Object obj=list.get(pos);
					propID.getField().set(obj,rs.getInt(1));
					pos++;
				}
				rs.close();
			}
			pre.close();
		}*/
		public void rollbackTransaction(FastDATransaction ts) {
			if(ts.getConn()==null) return;
			Connection conn=(Connection)ts.getConn();
			try {
				conn.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
}

