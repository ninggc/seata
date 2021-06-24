package io.seata.mongo.tx.mongo.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * @description: 对mongo数据处理的工具类
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/8 14:00
 * @version:v1.0
 */
public class MongoQueryUtil {


    /**
     * @param condition
     * @return org.springframework.data.mongodb.core.query.Query
     * @description: 依据查询条件构建查询器
     * @author cuijh
     * @date 2020/10/28 21:04
     */
    public static Query queryGenerator(Map<String, Object> condition) {
        Query query = new Query();
        if (condition == null || condition.size() == 0) {
            return query;
            // throw new BadRequestException(MetadataCode.MONGO_QUERY_GENERATOR_INVALID);
        }

        List<Entry<String, Object>> conditionList = new ArrayList<>(condition.entrySet());
        Criteria criteria = null;
        for (int i = 0; i < conditionList.size(); i++) {
            String key = conditionList.get(i).getKey();
            Object value = conditionList.get(i).getValue();
            if (i == 0) {
                if (value instanceof Collection) {
                    Collection<Object> collection = (Collection<Object>) value;
                    criteria = Criteria.where(key).in(collection.toArray());
                } else {
                    criteria = Criteria.where(key).is(value);
                }
                continue;
            }
            if (value instanceof Collection) {
                Collection<Object> collection = (Collection<Object>) value;
                criteria.and(conditionList.get(i).getKey()).in(collection.toArray());
            } else {
                criteria.and(conditionList.get(i).getKey()).is(value);
            }
        }

        query.addCriteria(criteria);
        return query;
    }

    /**
     * @param value
     * @return org.springframework.data.mongodb.core.query.Update
     * @description: 依据修改内容构建修改器
     * @author cuijh
     * @date 2020/10/28 22:05
     */
    public static Update updateGenerator(Map<String, Object> value) {
        if (value == null || value.size() == 0) {
            throw new RuntimeException("no value need to update");
        }

        Update update = new Update();
        value.entrySet().forEach(entry -> {
            update.set(entry.getKey(), entry.getValue());
        });

        return update;
    }

    /**
     * 获取mongo数据的id，支持map和ObjectValuePO类型
     * @param o
     * @return
     */
    public static String getMongoDataId(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof Map) {
            // TODO 2021/6/10 解析id的逻辑
            Map map = (Map) o;
            return Optional.of(map)
                .map(m -> m.get("id"))
                .map(String::valueOf)
                .orElseThrow(() -> new RuntimeException("id not found"));
        }

        throw new UnsupportedOperationException("不支持的类型: " + o.getClass().toString());
    }

    public static Object getMongoDataVersion(Object o) {
        if (o == null) {
            return null;
        }

        // TODO 2021/6/10 解析version
        if (o instanceof Map) {
            Map map = (Map) o;
            return Optional.of(map)
                .map(m -> m.get("version"))
                .map(String::valueOf)
                .orElseThrow(() -> new RuntimeException("no version found"));
        }

        throw new UnsupportedOperationException("不支持的类型: " + o.getClass().toString());
    }

}
