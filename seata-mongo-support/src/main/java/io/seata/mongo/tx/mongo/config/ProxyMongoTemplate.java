package io.seata.mongo.tx.mongo.config;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @description: mongoTemplate的代理
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/8 11:18
 * @version:v1.0
 */
public interface ProxyMongoTemplate {


    <T> T save(T objectToSave, String collectionName);

    List<Map> remove(Map<String, Object> condition, String collectionName);

    Map updateFirst(Map<String, Object> condition, Map<String, Object> value, String collectionName);

    <T> List<T> find(Map<String, Object> condition, Class<T> entityClass, String collectionName);

    <T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName);
}
