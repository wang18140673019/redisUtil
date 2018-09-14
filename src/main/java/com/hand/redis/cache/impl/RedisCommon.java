/*
 * #{copyright}#
 */
package com.hand.redis.cache.impl;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ziming.wang@hand-china.com
 */
@Component
public class RedisCommon implements SmartLifecycle {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected RedisTemplate<String, String> redisTemplate;
    private String category = "aurora:cache";


    @Autowired
    protected RedisSerializer<String> redisSerializer;

    @Bean
    public RedisSerializer<String> getRedisSerializer() {
        return new StringRedisSerializer();
    }




    public void setRedisSerializer(RedisSerializer<String> redisSerializer) {
        this.redisSerializer = redisSerializer;
    }







    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public RedisTemplate<String, String> getRedisTemplate() {
        return redisTemplate;
    }

    @Autowired
    public void setRedisTemplate(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }



    public String  getStringValue(String cacheName,String key) {
              return (String) getValue(cacheName,key,String.class);
    }


    public Object getMapValue(String cacheName,String key) {
        return (Object) getValue(cacheName,key,HashMap.class);
    }


    public String  getLongValue(String cacheName,String key) {
        return (String) getValue(cacheName,key,Long.class);
    }
    /**
     *
     * @param key redis key
     * @param returnType
     * @return
     */
    public Object getValue(String cacheName,String key,Class returnType) {
        return redisTemplate.execute((RedisCallback) (connection) -> {
            byte[] keyBytes = redisSerializer.serialize(getFullKey(cacheName,key));
            Map<byte[], byte[]> value = connection.hGetAll(keyBytes);
            if (value.size() == 0) {
                return  null;
            }
            try {
                Object bean = returnType.newInstance();
                for (Map.Entry<byte[], byte[]> entry : value.entrySet()) {
                    String pName = redisSerializer.deserialize(entry.getKey());
                    String pValue = redisSerializer.deserialize(entry.getValue());
                    if (bean instanceof Map) {
                        ((Map) bean).put(pName, pValue);
                        continue;
                    }
                    PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(bean, pName);
                    if (pd == null) {
                        continue;
                    }
                    Class<?> pType = pd.getPropertyType();
                    if (pType == java.util.Date.class) {
                        Long time = pValue.length() == 0 ? null : Long.parseLong(pValue);
                        BeanUtils.setProperty(bean, pName, time);
                    } else {
                        BeanUtils.setProperty(bean, pName, pValue);
                    }
                }
                return bean;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return  null;
        });
    }


    public void setBeanValue(String cacheName,String key,  Object value) {
        try {
            Map<String, Object> map = convertToMap(value);
            setMapValue(cacheName,key, map);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("setValue error ", e);
            }
        }
    }


    public void remove(String cacheName,String key) {
        redisTemplate.execute((RedisCallback) (connection) -> {
            byte[] keyBytes = redisSerializer.serialize(getFullKey(cacheName,key));
            connection.del(keyBytes);
            return  null;
        });
    }



    private void setMapValue(String cacheName,String key, Map<String, Object> value) {
        byte[] keyBytes = redisSerializer.serialize(getFullKey(cacheName,key));
        Map<byte[], byte[]> data = new HashMap<>();
        value.forEach((k, v) -> {
            // 排除特殊字段
            if (k.charAt(0) == '_') {
                return;
            }
            if (v instanceof java.util.Date) {
                v = ((Date) v).getTime();
            }
            if (v != null) {
                data.put(redisSerializer.serialize(k), redisSerializer.serialize(v.toString()));
            }
        });

        redisTemplate.execute((RedisCallback<Object>) (connection) -> {
            connection.hMSet(keyBytes, data);
            return null;
        });
    }





    private Map<String, Object> convertToMap(Object obj)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        Map<String, Object> map = PropertyUtils.describe(obj);
        map.remove("class"); // describe会包含 class 属性,此处无用
        return map;
    }

    protected String getFullKey(String cacheName,String key) {
        return new StringBuilder(getCategory()).append(":").append(cacheName).append(":").append(key).toString();
    }


    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {

    }

    @Override
    public void start() {
         if(redisTemplate ==null){
             throw new RuntimeException("redis 相关配置出错");
         }
         Map<String, Object> testMap = new HashMap<String,Object>();
              testMap.put("name","wangziming");
        setMapValue("test","wang",testMap);
              testMap.put("age","25");
         setMapValue("test","wang",testMap);
         testMap =(Map<String, Object>) getMapValue("test","wang");
         System.out.println("测试"+testMap);

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
