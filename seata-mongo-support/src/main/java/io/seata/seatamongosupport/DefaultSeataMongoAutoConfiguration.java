package io.seata.seatamongosupport;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import io.seata.mongo.tx.mongo.config.ProxyMongoTemplate;
import io.seata.mongo.tx.mongo.config.ProxyMongoTemplateImpl;
import io.seata.mongo.tx.support.MongoUndoLogImageTemplate;
import io.seata.spring.boot.autoconfigure.StarterConstants;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @description: SeataMongo自动装配
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/6/10 21:45
 * @version:v1.0
 */
@ConditionalOnClass({MongoTemplate.class})
@ConditionalOnProperty(prefix = StarterConstants.SEATA_PREFIX, name = "enabled", havingValue = "false", matchIfMissing = true)
@Configuration
public class DefaultSeataMongoAutoConfiguration {

    @Bean
    public MongoUndoLogImageTemplate mongoUndoLogImageTemplate() {
        return new MongoUndoLogImageTemplate() {
            @Override
            public Map singleExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<Map> supplier) {
                return supplier.get();
            }

            @Override
            public List<Map> multiExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<List<Map>> supplier) {
                return supplier.get();
            }

            @Override
            public <T> T executeInsert(String collectionName, Supplier<T> insertSupplier, String mongoDataId) {
                return insertSupplier.get();
            }

            @Override
            public <T> Collection<T> executeBatchInsert(String collectionName, Supplier<Collection<T>> insertSupplier) {
                return insertSupplier.get();
            }
        };
    }

    @Bean
    public ProxyMongoTemplate proxyMongoTemplate(MongoTemplate mongoTemplate, MongoUndoLogImageTemplate mongoUndoLogImageTemplate) {
        return new ProxyMongoTemplateImpl(mongoTemplate, mongoUndoLogImageTemplate);
    }
}
