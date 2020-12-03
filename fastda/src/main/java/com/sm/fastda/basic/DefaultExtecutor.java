package com.sm.fastda.basic;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sm.fastda.FastDAContext;
import com.sm.fastda.IDAEngine;
import com.sm.fastda.define.OBJAggregation;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJCompose;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;

public class DefaultExtecutor {
	private Map<String,IDAEngine> engines;
	public void setEngines(Map<String, IDAEngine> engines) {
		this.engines = engines;
	}
	public <T>  List<T> queryForList(Class<T> c,Map<String,String> params){
		FastDAContext context=FastDAContext.getValue();
		context.reset();
		try {
			OBJClass define = FastDAFactory.getInstance().init(c);
			context.define=define;
			context.params=params;
			//ast 判断引擎类型
			String majorEngineType=define.getSourceType();
			IDAEngine engine=this.engines.get(majorEngineType);
			if(engine==null) {
				context.log(1,"类型等于"+majorEngineType+"引擎为空,请检查引擎配置");
				return null;
			}
			List<Object> ids= engine.executeIndex(context);
			List<?> list=this.generateResult(define, ids);
			@SuppressWarnings("unchecked")
			List<T> result=(List<T>)list;
			context.status=true;
			return result;
		}catch (Exception e) {
			// TODO Auto-generated catch block
			context.log(e);
			//e.printStackTrace();
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T queryForObject(Class<T> c,Map<String,String> params,Object id) {
		FastDAContext context=FastDAContext.getValue();
		context.reset();
		try {
			OBJClass define=FastDAFactory.getInstance().init(c);
			context.define=define;
			context.params=params;
			//context.columnId=define.getSourceAlias()+"."+define.findProperty(define.findAllIdName()).getColumnName();
			List<Object> idlist=new ArrayList<Object>();
			idlist.add(id);
			List<?> result=generateResult(define,idlist);
			context.status=true;
			if(result.size()>0) return (T) result.get(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			context.log(e);
		}
		return null;
	}
	protected int _pageLimit=20;
	
	
	private List<?> generateResult(OBJClass define,List<Object> ids) throws Exception {
		List<?> result=queryOBJResult(define,define.findAllIdName(),ids);
		queryRelation(define,result,"");
		return result;
	}
	
	private void queryRelation(OBJClass define,List<?> result,String path) throws Exception {
		if(result==null||result.size()==0) return;
		//3.merge
		FastDAContext context=FastDAContext.getValue();
		//context.mustSave.add(define.findAllIdName());
		if(context.deep>9) return;
		String expand="";
		Map<String,String> params=context.params;
		if(params.containsKey("_expand")) expand=params.get("_expand");
		String[] temp=expand.split(",");
		for(int i=0;i<temp.length;i++) {
			String fieldName=temp[i];
			OBJRelation r=define.findRelation(fieldName);
			if(r==null) continue;
			//params.put("_excute.deep","1");
			//queryRelationObj(fieldName,define,result,params);
			queryRelationObj(r,result,path);
		}
		context.deep++;
	}
	
	private List<?> queryOBJResult(OBJClass define,String idfield,List<Object> ids) throws Exception {
		
		IDAEngine engine=this.engines.get(define.getSourceType());
		List<?> result=engine.queryOBJResult(define, idfield,ids);
		sortList(define,result,define.findAllIdName(),ids);
		return result;
	}
	
	private void sortList(OBJClass objc,List<?> result,String fieldName,List<Object> idlist) {
		if(result!=null&&result.size()>0) {
			Field f=objc.findProperty(fieldName).getField();
			Collections.sort(result,new Comparator<Object>() {
				@Override
				public int compare(Object arg0, Object arg1) {
					// TODO Auto-generated method stub
					int pos1=-1,pos2=-1;
					try {
						pos1 = idlist.indexOf(f.get(arg0));
						pos2=idlist.indexOf(f.get(arg1));
					} catch (IllegalArgumentException | IllegalAccessException e) {
					}
					if(pos1>pos2) return 1;
					else if(pos1<pos2) return -1;
					else return 0;
				}
			});
		}
	}
	
	private void queryRelationObj(OBJRelation r,List<?> result,String path) throws Exception {
		//Relation r=define.getRelations().get(fieldName);
		//String rfieldName=r.getLeftKey();
		OBJProperty leftp=r.getDefine().findProperty(r.getLeftKey());
		Field leftpf=leftp.getField();
		Set<Object> idset=new HashSet<>();
		for(int i=0;i<result.size();i++) {
			Object obj=result.get(i);
			Object idobj=leftpf.get(obj);
			if(idobj!=null)idset.add(idobj);
		}
		
		/*Iterator it=idset.iterator();*/
		List<Object> idlist=new ArrayList<Object>();
		idlist.addAll(idset);
		/*while(it.hasNext()) {
			idlist.add(it.next());
		}	*/	
		Field[] op=new Field[5];
		op[0]=leftpf;
		List<?> jointemp=null;
		if(!r.isDirect()) {
			//Class joinclz=Class.forName(r.joinObject());
			OBJClass joindefine=r.getJoinObject();
			jointemp=this.queryOBJResult(joindefine,r.getLeftJoin(), idlist);  //(joindefine,r.joinLeft(), idlist);
			Field joinR=joindefine.findProperty(r.getRightJoin()).getField();
			idset.clear();
			for(int i=0;i<jointemp.size();i++) {
				Object obj=jointemp.get(i);
				Object idobj=joinR.get(obj);
				if(idobj!=null)idset.add(idobj);
			}
			idlist.clear();
			idlist.addAll(idset);
			op[1]=joindefine.findProperty(r.getLeftJoin()).getField();
			op[2]=joinR;
		}
		
		IDAEngine engine=this.engines.get(r.getObjClass().getSourceType());
		String path1=null;
		if(path.equals("")) path1=r.getName();
		else path1=path+"."+r.getName();
		List<?> targetlist=engine.queryOBJResult(r.getObjClass(), r.getRightKey(),idlist,path1);
		//List<?> targetlist=this.queryOBJResult(r.getObjClass(),r.getRightKey(),idlist,r.getName());
		
		//next query relation
		queryRelation(r.getObjClass(),targetlist,path1);
		
		op[3]=r.getObjClass().findProperty(r.getRightKey()).getField();//define2.getFields().get(r.foreignKey());
		op[4]=r.getField();//define.getFields().get(fieldName);
		
		int rtype=getRelationType(r);
		
		copyObject(rtype,result,targetlist,jointemp,op);
	}
	
	private int getRelationType(OBJRelation r) {
		int rtype=0;
		if(r instanceof OBJCompose) rtype=2;
		else if(r instanceof OBJAggregation) rtype=1;
		return rtype;
	}
	
	private void copyObject(int rtype,List<?> orignlist,List<?> targetlist,List<?> templist,Field[] op) throws IllegalArgumentException, IllegalAccessException {
		if(targetlist==null) return;
		List<Object> leftlist=new ArrayList<>();
		for(int i=0;i<orignlist.size();i++) {
			Object obj=orignlist.get(i);
			Object leftval=op[0].get(obj);
			//10-22zengjia
			if(leftval==null) continue;
			if(rtype==0) {
				Object relationObj=null;
				if(templist==null) {
					relationObj=findObject(targetlist,leftval,op[3]);
				}else {
					Object temp1=findObject(templist,leftval,op[1]);
					Object temp2=op[2].get(temp1);
					relationObj=findObject(targetlist,temp2,op[3]);
				}
				if(relationObj!=null) op[4].set(obj, relationObj);
			}else {
				Object relationObj=null;
				leftlist.clear();
				leftlist.add(leftval);
				if(templist==null) {
					relationObj=findObjectList(targetlist,leftlist,op[3]);
				}else {
					List<Object> temp1=findObjectList(templist,leftlist,op[1]);
					List<Object> temp2=new ArrayList<>();
					for(int k=0;k<temp1.size();k++) {
						temp2.add(op[2].get(temp1.get(k)));
					}
					//Object temp2=op[2].get(temp1);
					relationObj=findObjectList(targetlist,temp2,op[3]);
				}
				if(relationObj!=null) op[4].set(obj, relationObj);
			}
			
		}
	}
	
	private Object findObject(List<?> list,Object leftval,Field f) throws IllegalArgumentException, IllegalAccessException {
		for(int i=0;i<list.size();i++) {
			Object obj=list.get(i);
			Object rightval=f.get(obj);
			if(leftval.equals(rightval)) return obj;
		}
		return null;
	}
	
	private List<Object> findObjectList(List<?> list,List<?> leftlist,Field f) throws IllegalArgumentException, IllegalAccessException {
		List<Object> result=new ArrayList<>();
		for(int i=0;i<list.size();i++) {
			for(int j=0;j<leftlist.size();j++) {
				Object leftval=leftlist.get(j);
				Object obj=list.get(i);
				Object rightval=f.get(obj);
				if(leftval.equals(rightval)) {
					result.add(obj);
				}
			}
		}
		return result;
	}
	//-------------------------------------------------------------------------------------------------------------------------
	public <T> void insertObject(T obj,Class<T> c) {
		List<T> list=new ArrayList<>(1);
		list.add(obj);
		insertBatchObject(list,c);
	}
	public <T> void insertBatchObject(List<T> list,Class<T> c) {
		if(list==null||list.size()==0) return;
		FastDAContext context=FastDAContext.getValue();
		context.reset();
		FastDATransaction ts=new FastDATransaction();
		IDAEngine engine=null;
		try {
			OBJClass define = FastDAFactory.getInstance().init(c);
			engine=this.engines.get(define.getSourceType());
			engine.startTransaction(ts);
			FastDAContext.getValue().transaction=ts;
			insertBatchObjectDo(list,define);
			engine.commitTransaction(ts);
			context.status=true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			context.log(e);
			engine.rollbackTransaction(ts);
			//e.printStackTrace();
		}
		
	}
	
	private <T> void insertBatchObjectDo(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		IDAEngine engine=this.engines.get(define.getSourceType());
		engine.insertBatch(list, define);
		updateRelationAll(list,define,"insert");
		
	}
	
	public <T> void updateObject(T obj,Class<T> c) {
		List<T> list=new ArrayList<>(1);
		list.add(obj);
		updateBatchObject(list,c);
	}
	
	
	public <T> void updateBatchObject(List<T> list,Class<T> c) {
		if(list==null||list.size()==0) return;
		FastDAContext context=FastDAContext.getValue();
		context.reset();
		FastDATransaction ts=new FastDATransaction();
		IDAEngine engine=null;
		try {
			OBJClass define = FastDAFactory.getInstance().init(c);
			engine=this.engines.get(define.getSourceType());
			engine.startTransaction(ts);
			FastDAContext.getValue().transaction=ts;
			updateBatchObjectDo(list,define);
			engine.commitTransaction(ts);
			context.status=true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			context.log(e);
			engine.rollbackTransaction(ts);
		}
	}
	private <T> void updateBatchObjectDo(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		IDAEngine engine=this.engines.get(define.getSourceType());
		List<T> list3=new ArrayList<>();
		OBJProperty pkprop=define.findProperty(define.findAllIdName());
		for(int i=0;i<list.size();i++) {
			Object val=pkprop.getField().get(list.get(i));
			if(val==null) {
				list3.add(list.get(i));
			}
		}
		engine.insertBatch(list3, define);
		//-----------add changed validate
		List<T> list2=null;
		try{
			//Field[] fs=define.getClz().getFields();
			Field ch=define.getClz().getField("changed");
			list2=new ArrayList<>();
			for(int i=0;i<list.size();i++) {
				Object val=ch.get(list.get(i));
				if(Boolean.TRUE.equals(val)) {
					list2.add(list.get(i));
				}
			}
		}catch(Exception e) {
			list2=list;
		}
		engine.updateBatch(list2, define);
		updateRelationAll(list,define,"update");
		
	}
	
	public <T> void deleteObject(T obj,Class<T> c) {
		List<T> list=new ArrayList<>(1);
		list.add(obj);
		deleteBatchObject(list,c);
	}
	public <T> void deleteBatchObject(List<T> list,Class<T> c) {
		if(list==null||list.size()==0) return;
		FastDAContext context=FastDAContext.getValue();
		context.reset();
		FastDATransaction ts=new FastDATransaction();
		IDAEngine engine=null;
		try {
			OBJClass define = FastDAFactory.getInstance().init(c);
			engine=this.engines.get(define.getSourceType());
			engine.startTransaction(ts);
			FastDAContext.getValue().transaction=ts;
			deleteBatchObjectDo(list,define);
			engine.commitTransaction(ts);
			context.status=true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			context.log(e);
			engine.rollbackTransaction(ts);
			//e.printStackTrace();
		}
	}
	
	private <T> void deleteBatchObjectDo(List<T> list,OBJClass define) throws Exception {
		if(list==null||list.size()==0) return;
		IDAEngine engine=this.engines.get(define.getSourceType());
		updateRelationAll(list,define,"delete");
		engine.deleteBatch(list, define);
		
	}
	
	private <T> void updateRelationAll(List<T> list,OBJClass define,String mode) throws Exception {
		Iterator<String> it=define.iteratorAllRelation();
		while(it.hasNext()) {
			String field=it.next();
			//FastDAContext context=FastDAContext.getValue();
			OBJRelation relation=define.findRelation(field);
			if(relation.isMaster()==false) continue;
			//reset
			//context.reset();
			//context.parentDefine=define;
			//context.parentList=list;
			//少 级联 组合下面的 关联，聚合删除
			
			if(mode.equals("insert")) {
				if(relation instanceof OBJCompose) {
					insertCompose(list,define,(OBJCompose)relation);
					//updateRelationAll(next,relation.getObjClass(),mode);
				}else if(relation.isDirect()==false) {
					addJoin(list,define,relation);
				}else {
					addRelation(list,define,relation);
				}
			}else if(mode.equals("delete")) {//重新考虑
				if(relation instanceof OBJCompose) {
					deleteCompose(list,define,relation);
				}else if(relation.isDirect()==false) {
					deleteRelation(list,define,relation);
				}else {
					deleteRelation(list,define,relation);
				}
			}else {
				if(relation instanceof OBJCompose) {
					List<Object> next=updateRelation(list,define,(OBJCompose)relation);
					this.updateBatchObjectDo(next, relation.getObjClass());
					//updateRelationAll(next,relation.getObjClass(),mode);
				}else if(relation.isDirect()==false) {
					updateJoin(list,define,relation);
				}else {
					updateRelation(list,define,relation);
				}
			}
			
			
			
			/*if(relation instanceof OBJCompose) {
				List<Object> next=null;
				if(mode.equals("delete")) {
					deleteComposePrepare(list,define,relation);
					updateRelationAll(null,relation.getObjClass(),mode);
					deleteCompose(relation);
				}else {
					if(mode.equals("insert")) next=insertCompose(list,define,(OBJCompose)relation);
					else
					next=updateRelation(list,define,relation);
					updateRelationAll(next,relation.getObjClass(),mode);
				}
				
			}else if(relation.isDirect()==false) {// join temp
				if(mode.equals("delete"))
					deleteJoin(list,define,relation);
				else
					updateJoin(list,define,relation);
			}else {
				if(mode.equals("delete"))
					deleteRelation(list,define,relation);
				else
					updateRelation(list,define,relation);
			}*/
		}
	}
	
	private <T> List<Object> insertCompose(List<T> list,OBJClass define,OBJCompose relation) throws Exception  {
		OBJClass childdefine=relation.getObjClass();
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		OBJProperty fkprop2=childdefine.findProperty(relation.getRightKey());
		List<Object> result=new ArrayList<>();
		List<Integer> indexs=new ArrayList<>();
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object robj=relation.getField().get(obj);
			if(robj==null) continue;
			Object fkvalue=fkprop.getField().get(obj);
			List<?> childlist=(List<?>)robj;
			for(int j=0;j<childlist.size();j++) {
				Object childobj=childlist.get(j);
				fkprop2.getField().set(childobj, fkvalue);
				indexs.add(i);
				result.add(childobj);
			}
		}
		if(result.size()==0) return result;
		/*FastDAContext context=FastDAContext.getValue();
		context.parentDefine=define;
		context.parentList=list;
		context.indexs=indexs;
		context.relation=relation;*/
		//IDAEngine engine=this.engines.get(childdefine.getSourceType());
		//engine.insertBatch(result, childdefine);
		this.insertBatchObjectDo(result, childdefine);
		return result;
	}
	
	
	private <T> List<Object> addRelation(List<T> list,OBJClass define,OBJRelation relation) throws Exception {
		OBJClass childdefine=relation.getObjClass();
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		OBJProperty fkprop2=childdefine.findProperty(relation.getRightKey());
		List<Object> result=new ArrayList<>();
		List<Integer> indexs=new ArrayList<>();
		//Set<Object> fkset=new HashSet<>();
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object robj=relation.getField().get(obj);
			if(robj==null) continue;
			Object fkvalue=fkprop.getField().get(obj);
			//fkset.add(fkvalue);
			if(robj instanceof List) {
				List<?> childlist=(List<?>)robj;
				for(int j=0;j<childlist.size();j++) {
					Object childobj=childlist.get(j);
					fkprop2.getField().set(childobj, fkvalue);
					indexs.add(i);
					result.add(childobj);
				}
			}else {
				fkprop2.getField().set(robj, fkvalue);
				indexs.add(i);
				result.add(robj);
			}
			
		}
		if(result.size()==0) return result;
		IDAEngine engine=this.engines.get(childdefine.getSourceType());
		/*FastDAContext context=FastDAContext.getValue();
		context.parentDefine=define;
		context.parentList=list;
		context.indexs=indexs;
		context.relation=relation;*/
		engine.addSimpleRelation(result, childdefine, relation);
		/*if(relation instanceof OBJAggregation) {
			context.relationType="Aggregation";
		}else if(relation instanceof OBJCompose) {
			context.relationType="Compose";
		}else context.relationType="Relation";
		engine.updateRelation();*/
		
		return result;
	}
	private <T> void addJoin(List<T> list,OBJClass define,OBJRelation relation) throws Exception {
		OBJClass childdefine=relation.getObjClass();
		//OBJProperty prop=define.findProperty(relation.getName());
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		OBJProperty fkprop2=childdefine.findProperty(relation.getRightKey());
		
		OBJClass joindefine=relation.getJoinObject();
		OBJProperty joinprop1=joindefine.findProperty(relation.getLeftJoin());
		OBJProperty joinprop2=joindefine.findProperty(relation.getRightJoin());
		List<Object> result=new ArrayList<>();
		List<Integer> indexs=new ArrayList<>();
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object robj=relation.getField().get(obj);
			if(robj==null) continue;
			Object fkvalue=fkprop.getField().get(obj);
			if(robj instanceof List) {
				List<?> childlist=(List<?>)robj;
				for(int j=0;j<childlist.size();j++) {
					Object childobj=childlist.get(j);
					Object fkvalue2=fkprop2.getField().get(childobj);
					Object joinobj=joindefine.getClz().newInstance();
					joinprop1.getField().set(joinobj, fkvalue);
					joinprop2.getField().set(joinobj, fkvalue2);
					indexs.add(i);
					result.add(joinobj);
				}
			}else {
				Object fkvalue2=fkprop2.getField().get(robj);
				Object joinobj=joindefine.getClz().newInstance();
				joinprop1.getField().set(joinobj, fkvalue);
				joinprop2.getField().set(joinobj, fkvalue2);
				indexs.add(i);
				result.add(joinobj);
			}
			
		}
		if(result.size()==0) return;
		IDAEngine engine=this.engines.get(joindefine.getSourceType());
		/*FastDAContext context=FastDAContext.getValue();
		context.parentDefine=define;
		context.parentList=list;
		context.indexs=indexs;
		context.relation=relation;*/
		engine.insertBatch(result, joindefine);
		/*context.curdefine=joindefine;
		context.list=result;
		context.pkName=relation.getRightJoin();
		context.fkName=relation.getLeftJoin();
		context.joinFlag=true;
		if(relation instanceof OBJAggregation) {
			context.relationType="Aggregation";
		}else if(relation instanceof OBJCompose) {
			context.relationType="Compose";
		}else context.relationType="Relation";
		engine.updateRelation();*/
		
	}
	private <T> List<Object> updateRelation(List<T> list,OBJClass define,OBJRelation relation) throws IllegalArgumentException, IllegalAccessException {
		
		OBJClass childdefine=relation.getObjClass();
		List<Object> result=new ArrayList<>();
		List<Integer> indexs=new ArrayList<>();
		boolean flag=false;
		OBJProperty fkprop1=define.findProperty(relation.getLeftKey());
		OBJProperty fkprop2=childdefine.findProperty(relation.getRightKey());
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object robj=relation.getField().get(obj);
			if(robj==null) continue;
			Object fkvalue=fkprop1.getField().get(obj);
			flag=true;
			if(robj instanceof List) {
				List<?> childlist=(List<?>)robj;
				for(int j=0;j<childlist.size();j++) {
					Object childobj=childlist.get(j);
					fkprop2.getField().set(childobj, fkvalue);
					indexs.add(i);
					result.add(childobj);
				}
			}else {
				indexs.add(i);
				fkprop2.getField().set(robj, fkvalue);
				result.add(robj);
			}
			
		}
		if(flag==false) return null;
		IDAEngine engine=this.engines.get(childdefine.getSourceType());
		/*FastDAContext context=FastDAContext.getValue();
		context.parentDefine=define;
		context.parentList=list;
		context.indexs=indexs;
		context.relation=relation;*/
		//context.fkName=relation.getLeftKey();
		try {
			engine.updateAllRelation(result,childdefine,relation);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	private <T> void updateJoin(List<T> list,OBJClass define,OBJRelation relation) throws Exception {
		OBJClass childdefine=relation.getObjClass();
		//OBJProperty prop=relation.get
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		OBJProperty fkprop2=childdefine.findProperty(relation.getRightKey());
		
		OBJClass joindefine=relation.getJoinObject();
		OBJProperty joinprop1=joindefine.findProperty(relation.getLeftJoin());
		OBJProperty joinprop2=joindefine.findProperty(relation.getRightJoin());
		List<Object> result=new ArrayList<>();
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object robj=relation.getField().get(obj);
			if(robj==null) continue;
			Object fkvalue=fkprop.getField().get(obj);
			if(robj instanceof List) {
				List<?> childlist=(List<?>)robj;
				for(int j=0;j<childlist.size();j++) {
					Object childobj=childlist.get(j);
					Object fkvalue2=fkprop2.getField().get(childobj);
					Object joinobj=joindefine.getClz().newInstance();
					joinprop1.getField().set(joinobj, fkvalue);
					joinprop2.getField().set(joinobj, fkvalue2);
					result.add(joinobj);
				}
			}else {
				Object fkvalue2=fkprop2.getField().get(robj);
				Object joinobj=joindefine.getClz().newInstance();
				joinprop1.getField().set(joinobj, fkvalue);
				joinprop2.getField().set(joinobj, fkvalue2);
				result.add(joinobj);
			}
			
		}
		if(result.size()==0) return;
		IDAEngine engine=this.engines.get(joindefine.getSourceType());
		engine.updateAllRelation(result,joindefine,relation);
		/*FastDAContext context=FastDAContext.getValue();
		context.curdefine=joindefine;
		context.list=result;
		context.pkName=relation.getRightJoin();
		context.fkName=relation.getLeftJoin();
		context.joinFlag=true;
		if(relation instanceof OBJAggregation) {
			context.relationType="Aggregation";
		}else if(relation instanceof OBJCompose) {
			context.relationType="Compose";
		}else context.relationType="Relation";
		engine.updateRelation();*/
		
	}
	
	private <T> void deleteRelation(List<T> list,OBJClass define,OBJRelation relation) throws Exception {
		Set<Object> fkset=new HashSet<>();
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object fkvalue=fkprop.getField().get(obj);
			fkset.add(fkvalue);
		}
		if(fkset.size()==0) return;
		List<Object> idList=new ArrayList<>();
		idList.addAll(fkset);
		
		IDAEngine engine=this.engines.get(define.getSourceType());
		engine.deleteRelation(idList,relation);
		
	}
	
	private <T> void deleteCompose(List<T> list,OBJClass define,OBJRelation relation) throws Exception {
		Set<Object> fkset=new HashSet<>();
		OBJProperty fkprop=define.findProperty(relation.getLeftKey());
		for(int i=0;i<list.size();i++) {
			T obj=list.get(i);
			Object fkvalue=fkprop.getField().get(obj);
			fkset.add(fkvalue);
		}
		if(fkset.size()==0) return;
		List<Object> idList=new ArrayList<>();
		idList.addAll(fkset);
		IDAEngine engine=this.engines.get(relation.getObjClass().getSourceType());
		List<?> result=engine.queryDeleteCompose(relation.getObjClass(), relation.getRightKey(), idList);
		this.deleteBatchObjectDo(result, relation.getObjClass());
		
	}
	
	/*private <T> void deleteCompose(OBJRelation relation) throws IllegalArgumentException, IllegalAccessException {
		FastDAContext context=FastDAContext.getValue();
		context.curdefine=relation.getObjClass();
		context.relationType="Compose";
		IDAEngine engine=this.engines.get(context.curdefine.getSourceType());
		engine.deleteRelation();
		context.deletePath.remove(relation.getName());
	}*/
	
	
	
	
	
}
