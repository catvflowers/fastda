package com.sm.fastda.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface Aggregation {
	String parentKey() default "id";
	String foreignKey() default "id";
	boolean require() default false;
	String joinLeft() default "";
	String joinRight() default "";
	String joinObject() default "";
}
