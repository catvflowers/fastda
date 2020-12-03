package com.sm.fastda;

import java.util.List;
import java.util.Map;

import com.sm.fastda.basic.DefaultExtecutor;

public class FastDAExtecutor {
	private DefaultExtecutor de;
	public FastDAExtecutor() {
		de=new DefaultExtecutor();
	}
	
	public void setEngines(Map<String, IDAEngine> engines) {
		this.de.setEngines(engines);
	}
	
	public <T>  List<T> queryForList(Class<T> c,Map<String,String> params){
		return de.queryForList(c, params);
	}
	
	public <T> T queryForObject(Class<T> c,Map<String,String> params,Object id) {
		return de.queryForObject(c, params, id);
	}
	
	//------------------------------------------------------------------------------------
	public <T> void insertBatchObject(List<T> list,Class<T> c) {
		this.de.insertBatchObject(list, c);
	}
	public <T> void updateBatchObject(List<T> list,Class<T> c) {
		this.de.updateBatchObject(list, c);
	}
	public <T> void deleteBatchObject(List<T> list,Class<T> c) {
		this.de.deleteBatchObject(list, c);
	}
	
	
}
