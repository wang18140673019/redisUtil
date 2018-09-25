package com.hand.redis.cache.annotation;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
/**
 *  加了该注解的DTO,某些方法才会被执行
 * @author 18771
 *
 */
public @interface RedisBean {}