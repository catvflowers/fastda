package com.sm.fastda.basic;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.sm.fastda.annotation.Aggregation;
import com.sm.fastda.annotation.Column;
import com.sm.fastda.annotation.Compose;
import com.sm.fastda.annotation.Id;
import com.sm.fastda.annotation.Relation;
import com.sm.fastda.annotation.Source;
import com.sm.fastda.define.OBJAggregation;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJCompose;
import com.sm.fastda.define.OBJProperty;
import com.sm.fastda.define.OBJRelation;



public class FastDAFactory {
	
	private static FastDAFactory  instance=null;
	private static int count=0;
	private static Map<String,OBJClass> caches=new HashMap<>();
	//private Map<String,IDAEngine> engines;	
	public static FastDAFactory getInstance() {
		if(instance==null) instance=new FastDAFactory();
		return instance;
	}
	
	public FastDAFactory() {
		//pool= ClassPool.getDefault();
	}

	public  OBJClass init(Class<?> c) throws ClassNotFoundException {
		String name=c.getName();
		if(caches.containsKey(name)) return caches.get(name);
		
		boolean clzHasAnno = c.isAnnotationPresent(Source.class);
		if(!clzHasAnno) return null;
		
		Source annoSource=(Source)c.getAnnotation(Source.class);
		
		OBJClass objc=new OBJClass();
		Class<?> superc=c.getSuperclass();
		if(!superc.getSimpleName().equals("Object")) {
			OBJClass superobjc=init(superc);
			objc.setParent(superobjc);
		}else {
			/*CtClass ctClass = pool.get(name);
			ctClass.addField(CtField.make("private boolean changed;", ctClass));
			c=ctClass.toClass();*/
		}
		
		objc.setClz(c);
		objc.setName(c.getSimpleName());
		objc.setSourceType(annoSource.type());
		objc.setSourceName(annoSource.name());
		count++;
		objc.setSourceAlias("t"+count);
		
		Field[] fields=c.getDeclaredFields();
		for(int i=0;i<fields.length;i++) {
			Field field=fields[i];
			if (!field.isAccessible())
				field.setAccessible(true);
			
			
			
			boolean fieldHasAnno = field.isAnnotationPresent(Column.class);
			if(fieldHasAnno) {
				Column annoColumn =field.getAnnotation(Column.class);
				OBJProperty objp=new OBJProperty();
				objp.setDefine(objc);
				objp.setField(field);
				objp.setName(field.getName());
				objp.setType(field.getType().getSimpleName());
				objp.setLevel(annoColumn.level());
				String columnname=annoColumn.name().equals("")?field.getName():annoColumn.name();
				objp.setColumnName(columnname);
				String columntype=annoColumn.type().equals("")?objp.getType():annoColumn.type();
				objp.setColumnType(columntype);
				objc.addProperty(objp);
			}
				//this.columns.put(field.getName(), field.getAnnotation(Column.class));
			
			boolean hasId=field.isAnnotationPresent(Id.class);
			if(hasId) {
				objc.setIdName(field.getName());
			}
			
			/*this.fields.put(field.getName(),field);
			boolean fieldHasAnno = field.isAnnotationPresent(Column.class);
			if(fieldHasAnno)
				this.columns.put(field.getName(), field.getAnnotation(Column.class));*/
			
			/*boolean hasId=field.isAnnotationPresent(Id.class);
			if(hasId) {
				this._idName=field.getName();
				this.id=field.getAnnotation(Id.class);
			}*/
			caches.put(name, objc);
			boolean hasRelation=field.isAnnotationPresent(Relation.class);
			if(hasRelation) {
				Relation r=field.getAnnotation(Relation.class);
				OBJRelation objr=new OBJRelation();
				objr.setMaster(r.master());
				objr.setDefine(objc);
				objr.setField(field);
				objr.setLeftKey(r.parentKey());
				
				String joinclassname=r.joinObject();
				if(joinclassname!=null&&!joinclassname.equals("")) {
					Class<?> joinc=Class.forName(joinclassname);
					OBJClass joinobjc=init(joinc);
					objr.setJoinObject(joinobjc);
					
					objr.setLeftJoin(r.joinLeft());
					objr.setRightJoin(r.joinRight());
				}
				objr.setRightKey(r.foreignKey());
				objr.setName(field.getName());
				objr.setObjClass(init(field.getType()));
				
				objc.addRelation(objr);
			}
			boolean hasAggregation=field.isAnnotationPresent(Aggregation.class);
			if(hasAggregation) {
				Aggregation r=field.getAnnotation(Aggregation.class);
				OBJAggregation objr=new OBJAggregation();
				objr.setDefine(objc);
				objr.setField(field);
				objr.setLeftKey(r.parentKey());
				
				String joinclassname=r.joinObject();
				if(joinclassname!=null&&!joinclassname.equals("")) {
					Class<?> joinc=Class.forName(joinclassname);
					OBJClass joinobjc=init(joinc);
					objr.setJoinObject(joinobjc);
					
					objr.setLeftJoin(r.joinLeft());
					objr.setRightJoin(r.joinRight());
				}
				objr.setRightKey(r.foreignKey());
				objr.setName(field.getName());
				Type type=field.getGenericType();
				Class<?> entityClass = (Class<?>)((ParameterizedType)type).getActualTypeArguments()[0];
				objr.setObjClass(init(entityClass));
				objc.addRelation(objr);
			}
			boolean hasCompose=field.isAnnotationPresent(Compose.class);
			if(hasCompose) {
				Compose r=field.getAnnotation(Compose.class);
				OBJCompose objr=new OBJCompose();
				objr.setDefine(objc);
				objr.setField(field);
				objr.setLeftKey(r.parentKey());
				objr.setRightKey(r.foreignKey());
				objr.setName(field.getName());
				Type type=field.getGenericType();
				Class<?> entityClass = (Class<?>)((ParameterizedType)type).getActualTypeArguments()[0];
				objr.setObjClass(init(entityClass));
				objc.addRelation(objr);
			}
			
		}
		
		return objc;
	}
	
	
	
	/*private IDAEngine getDAEngine(String sourcetype) {
		return this.engines.get(sourcetype);
	}*/
}
