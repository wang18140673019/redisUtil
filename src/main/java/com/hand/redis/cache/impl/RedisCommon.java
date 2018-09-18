/*
 * #{copyright}#
 */
package com.hand.redis.cache.impl;

import com.hand.redis.model.User;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 存的都是字符串
 * @author ziming.wang@hand-china.com
 */
@Component
public class RedisCommon implements SmartLifecycle {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected StringRedisTemplate redisTemplate;
    private String category = "aurora:cache";


    @Autowired
    protected RedisSerializer<String> redisSerializer;

    @Bean
    public RedisSerializer<String> getRedisSerializer() {
        return new StringRedisSerializer();
    }


    /**
     *
     redisTemplate.opsForValue();//操作字符串
     redisTemplate.opsForHash();//操作hash
     redisTemplate.opsForList();//操作list
     redisTemplate.opsForSet();//操作set
     redisTemplate.opsForZSet();//操作有序set

     */




    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public RedisTemplate<String, String> getRedisTemplate() {
        return redisTemplate;
    }




    public void setStringValue(String cacheName,String key,  String value){
        String fullKey= getFullKey(cacheName,key);
        redisTemplate.opsForValue().set(fullKey,value);
    }

    public void  setObjectValue(String cacheName,String key, Object value) {
        String str= String.valueOf(value);
        setStringValue(cacheName,key,str);

    }
    public void  setLongValue(String cacheName,String key,  Long value) {
        String str= String.valueOf(value);
        setStringValue(cacheName,key,str);

    }

    public void  setFloatValue(String cacheName,String key, Float value) {
        String str= String.valueOf(value);
        setStringValue(cacheName,key,str);
    }

    public void  setIntegerValue(String cacheName,String key, Integer value) {
        String str= String.valueOf(value);
        setStringValue(cacheName,key,str);
    }

    public void  setDoubleValue(String cacheName,String key, Double value) {
        String str= String.valueOf(value);
        setStringValue(cacheName,key,str);
    }

    public void setMapValue(String cacheName,String key,Map value) {
        String fullKey= getFullKey(cacheName,key);
        redisTemplate.opsForHash().putAll(fullKey, value);
    }

    public Map getMapValue(String cacheName,String key) {
        return (Map) redisTemplate.execute((RedisCallback) (connection) -> {
            byte[] keyBytes = redisSerializer.serialize(getFullKey(cacheName,key));
            Map<byte[], byte[]> value = connection.hGetAll(keyBytes);
            Map returnMap= new HashMap();
            if (value.size() == 0) {
                return  null;
            }
                for (Map.Entry<byte[], byte[]> entry : value.entrySet()) {
                    String pName = redisSerializer.deserialize(entry.getKey());
                    String pValue = redisSerializer.deserialize(entry.getValue());
                    returnMap.put(pName, pValue);

                }

            return  returnMap;
        });
    }


    public String getStringValue(String cacheName,String key){
        String fullKey= getFullKey(cacheName,key);
        return (String)redisTemplate.opsForValue().get(fullKey);
    }

    public Long  getLongValue(String cacheName,String key) {
          String longStr = getStringValue(cacheName,key);
        return Long.parseLong(longStr);
    }

    public Double  getDoubleValue(String cacheName,String key) {
        String doubleStr = getStringValue(cacheName,key);
        return Double.parseDouble(doubleStr);
    }

    public Integer  getIntegerValue(String cacheName,String key) {
        String integerStr = getStringValue(cacheName,key);
        return Integer.parseInt(integerStr);
    }

    public Float  getFloatValue(String cacheName,String key) {
        String floatStr = getStringValue(cacheName,key);
        return Float.parseFloat(floatStr);
    }


    /**
     *
     * @param cacheName namespace
     * @param key
     * @param returnType bean 对象(实体类)
     * @param <T>
     * @return
     */
    public <T> T  getBeanValue(String cacheName,String key,Class returnType) {

        return(T) redisTemplate.execute((RedisCallback) (connection) -> {
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
                return (T) bean;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return null;
        });
    }

    /**
     *
     * @param cacheName redis namespace
     * @param key
     * @param value 存入redis 的实体类
     */
    public <T> void setBeanValue(String cacheName,String key, T value) {
        Map<String, Object> map =null;
        try {
            map = convertToMap(value);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("setValue error ", e);
            }
        }
        setbeanToMap(cacheName,key,map);



    }




    private void setbeanToMap(String cacheName,String key, Map<String, Object> value) {
        if(value==null) return;
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






    private <T> Map<String, Object> convertToMap(T obj)
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
        logger.debug("开始测试");
         String cacheName ="test";
         String name = "王子明";
         setStringValue(cacheName,"name",name);
        name= getStringValue(cacheName,"name");
        logger.warn("name :{}",name);

         Map<String, Object> testMap = new HashMap<String,Object>();
              testMap.put("name","wangziming"+System.currentTimeMillis());
        setMapValue("test","wang",testMap);
              testMap.put("age","25");
         setMapValue(cacheName,"wang",testMap);
         testMap =(Map<String, Object>) getMapValue(cacheName,"wang");
        logger.warn("testMap :{}",testMap);

        setObjectValue(cacheName,"age",12L);
        Long age = getLongValue(cacheName,"age");
        logger.warn("age :{}",age);
        User user = new User();
        user.setName("石锐");
        user.setAge(18L);
        setBeanValue(cacheName,"user",user);
        user = (User) getBeanValue(cacheName,"user",User.class);
        logger.warn("user name:{} age {}",user.getName(),user.getAge());

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
