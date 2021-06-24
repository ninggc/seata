package io.seata.mongo.tx.mongo.config;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import io.seata.mongo.tx.mongo.util.MongoQueryUtil;
import io.seata.mongo.tx.support.MongoUndoLogImageTemplate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;

/**
 * @description: mongo repo需要使用代理操作mongo，代理会对mongo的增删改操作生成前后镜像
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/8 11:18
 * @version:v1.0
 */
@Slf4j
public class ProxyMongoTemplateImpl implements ProxyMongoTemplate {

    private final MongoTemplate mongoTemplate;
    private final MongoUndoLogImageTemplate mongoUndoLogImageTemplate;

    public ProxyMongoTemplateImpl(MongoTemplate mongoTemplate, MongoUndoLogImageTemplate mongoUndoLogImageTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.mongoUndoLogImageTemplate = mongoUndoLogImageTemplate;
    }

    @Override
    public <T> T save(T objectToSave, String collectionName) {
        Supplier<T> insertSupplier = () -> mongoTemplate.save(objectToSave, collectionName);

        return mongoUndoLogImageTemplate.executeInsert(collectionName, insertSupplier, MongoQueryUtil.getMongoDataId(objectToSave));
    }

    @Override
    public List<Map> remove(Map<String, Object> condition, String collectionName) {
        Supplier<List<Map>> deleteResultSupplier = () -> mongoTemplate.findAllAndRemove(MongoQueryUtil.queryGenerator(condition), Map.class, collectionName);

        return mongoUndoLogImageTemplate.multiExecuteWithCondition(collectionName, OpeType.DELETE, condition, deleteResultSupplier);
    }

    @Override
    public Map updateFirst(Map<String, Object> condition, Map<String, Object> value, String collectionName) {
        Supplier<Map> supplier = () -> {
            // 移除version的设置值
            // TODO 2021/6/24 找到version对应的列
            value.remove("version");
            Update update = MongoQueryUtil.updateGenerator(value);
            // 设置version自增
            update.inc("version");
            return mongoTemplate.findAndModify(MongoQueryUtil.queryGenerator(condition), update, Map.class, collectionName);
        };

        return mongoUndoLogImageTemplate.singleExecuteWithCondition(collectionName, OpeType.UPDATE, condition, supplier);
    }

    @Override
    public <T> List<T> find(Map<String, Object> condition, Class<T> entityClass, String collectionName) {
        // DON'T GENERATE TX_IMAGE
        return mongoTemplate.find(MongoQueryUtil.queryGenerator(condition), entityClass, collectionName);
    }

    @Override
    public <T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName) {
        Supplier<Collection<T>> batchInsertSupplier = () -> mongoTemplate.insert(batchToSave, collectionName);

        return mongoUndoLogImageTemplate.executeBatchInsert(collectionName, batchInsertSupplier);
    }
}
