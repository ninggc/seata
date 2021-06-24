package io.seata.mongo.tx;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import io.seata.mongo.tx.support.MongoLockRetryPolicy;
import io.seata.mongo.tx.support.MongoSeataRmHelper;
import io.seata.mongo.tx.support.MongoUndoLogImageTemplate;
import io.seata.spring.boot.autoconfigure.SeataAutoConfiguration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @description: TODO
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/31 17:49
 * @version:v1.0
 */
@ConditionalOnMissingBean(SeataAutoConfiguration.class)
@Configuration
public class DefaultConfiguration {
    @Bean
    public MongoSeataRmHelper mongoSeataRmHelper(DataSource dataSource) {
        return new MongoSeataRmHelper(dataSource) {

        };
    }

    @Bean
    public MongoLockRetryPolicy mongoLockRetryPolicy() {
        return new MongoLockRetryPolicy();
    }

    @Bean
    public MongoUndoLogImageTemplate mongoUndoLogImageTemplate(MongoTemplate mongoTemplate, MongoSeataRmHelper mongoSeataRmHelper) {
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
}
