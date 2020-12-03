package com.sm.fastda;

import java.util.List;

import com.sm.fastda.basic.FastDATransaction;
import com.sm.fastda.define.OBJClass;
import com.sm.fastda.define.OBJRelation;

public interface IDAEngine {
	public List<Object> executeIndex(FastDAContext context)throws Exception;
	public List<?> queryOBJResult(OBJClass define,String idfield,List<Object> ids)throws Exception ;
	public List<?> queryOBJResult(OBJClass define,String idfield,List<Object> ids,String relationName)throws Exception ;
	public List<?> queryDeleteCompose(OBJClass define,String fkfield,List<Object> ids)throws Exception ;
	
	public <T> void insertBatch(List<T> list,OBJClass define)throws Exception;
	public <T> void updateBatch(List<T> list,OBJClass define)throws Exception;
	public <T> void deleteBatch(List<T> list,OBJClass define)throws Exception;
	public  void addSimpleRelation(List<?> list,OBJClass define,OBJRelation relation)throws Exception;
	public  void updateAllRelation(List<?> list,OBJClass define,OBJRelation relation)throws Exception;
	public  void deleteRelation(List<?> list,OBJRelation relation)throws Exception;
	
	public void startTransaction(FastDATransaction ts);
	public void commitTransaction(FastDATransaction ts);
	public void rollbackTransaction(FastDATransaction ts);
}
