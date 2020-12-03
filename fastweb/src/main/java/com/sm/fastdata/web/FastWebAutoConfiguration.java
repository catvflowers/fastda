package com.sm.fastdata.web;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.condition.PatternsRequestCondition;
import org.springframework.web.servlet.mvc.condition.RequestMethodsRequestCondition;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import com.sm.fastda.FastDAExtecutor;
import com.sm.fastda.IDAEngine;
import com.sm.fastda.db.MySQLEngine;

@Configuration
@ConditionalOnWebApplication()
@AutoConfigureOrder(Ordered.LOWEST_PRECEDENCE - 10)
@AutoConfigureAfter({ WebMvcAutoConfiguration.class,DataSourceAutoConfiguration.class})
public class FastWebAutoConfiguration {
	
	@Autowired
    private ApplicationContext applicationContext;
	
	@Autowired
    private  RequestMappingHandlerMapping requestMappingHandlerMapping;
	
	@Autowired
	DataSource dataSource;
	
	private FastDAExtecutor extecutor;
	@Bean
    @ConditionalOnMissingBean
    public FastConfiguration init() {
		String[] beanNames=applicationContext.getBeanNamesForType(Object.class);
		for (String beanName : beanNames) {
		      if (isHandler(applicationContext.getType(beanName))){
		    	  if(isHandler2(applicationContext.getType(beanName))){
		    		  detectHandlerMethods(beanName,Boolean.TRUE);
		    	  }else{
		    		  detectHandlerMethods(beanName,Boolean.FALSE);
		    	  }
		      }
		  }
		
		return new FastConfiguration();
	}
	
    public FastDAExtecutor getExtecutor() {
    	if(extecutor!=null) return extecutor;
		extecutor=new FastDAExtecutor();
		MySQLEngine mysqlengine=new MySQLEngine();
		mysqlengine.setDataSource(dataSource);
		Map<String,IDAEngine> engines=new HashMap<>();
		engines.put("rdb", mysqlengine);
		extecutor.setEngines(engines);
		return extecutor;
	}
	
	private boolean isHandler(Class<?> beanType) {
		if(beanType.getAnnotation(FastController.class)!=null)
			return true;
		else return false;
	}
	private boolean isHandler2(Class<?> beanType) {
		if(beanType.getAnnotation(FastAggregationService.class)!=null)
			return true;
		else return false;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void detectHandlerMethods(String beanName,Boolean flag) {
		Class<?> beanType=applicationContext.getType(beanName);
		FastController controller=beanType.getAnnotation(FastController.class);
		//String path=controller.path();
		String url=controller.url();
		FastDAExtecutor extecutor=getExtecutor();
		FastControllerImpl handler=new FastControllerImpl();//applicationContext.getBean(FastControllerImpl.class);//new FastControllerImpl();
		if(flag){
			FastAggregationService fastAggregationService = beanType.getAnnotation(FastAggregationService.class);
			String value = fastAggregationService.value();
			handler.setUrlList(getUrlAndType(value));
		}
		handler.setClz(beanType);
		handler.setExtecutor(extecutor);
		String[] urls=new String[1];
		urls[0]=url;
		System.out.println("RegisterMapping FastWeb url="+url);
		PatternsRequestCondition prc=new PatternsRequestCondition(urls);
		Method[] methods=FastControllerImpl.class.getMethods();
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc,new RequestMethodsRequestCondition(RequestMethod.POST),null,null,null,null,null), handler, find(methods,"insertAll"));
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc,new RequestMethodsRequestCondition(RequestMethod.GET),null,null,null,null,null), handler, find(methods,"queryAll"));
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc,new RequestMethodsRequestCondition(RequestMethod.PATCH),null,null,null,null,null), handler, find(methods,"updateAll"));
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc,new RequestMethodsRequestCondition(RequestMethod.DELETE),null,null,null,null,null), handler, find(methods,"deleteAll"));
		
		String[] urls2=new String[1];
		urls2[0]=url+"/{id}";
		PatternsRequestCondition prc2=new PatternsRequestCondition(urls2);
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc2,new RequestMethodsRequestCondition(RequestMethod.GET),null,null,null,null,null), handler, find(methods,"query"));
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc2,new RequestMethodsRequestCondition(RequestMethod.PATCH),null,null,null,null,null), handler, find(methods,"update"));
		requestMappingHandlerMapping.registerMapping(new RequestMappingInfo(prc2,new RequestMethodsRequestCondition(RequestMethod.DELETE),null,null,null,null,null), handler, find(methods,"delete"));
	}
	
	private Method find(Method[] methods,String name) {
		for(int i=0;i<methods.length;i++) {
			if(methods[i].getName().equals(name)) {
				return methods[i];
			}
		}
		return null;
	}
	private List<Map<String,String>> getUrlAndType(String value){
		List<Map<String,String>> list = new ArrayList<>();
		String[] urls = value.split(";");
		for (String url : urls) {
			String[] str = url.split(",");
			Map<String,String> map = new HashMap<>();
			if(str.length==2) {
				map.put("type", str[0]);
				map.put("url", str[1]);
				list.add(map);
			}
		}
		return list;
	}
}
