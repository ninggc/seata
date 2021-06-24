package io.seata.seatamongosupport;

import io.seata.mongo.tx.mongo.component.MongoUndoLogOpeService;
import io.seata.mongo.tx.mongo.config.PlainMongoTransactionManager;
import io.seata.mongo.tx.mongo.config.ProxyMongoTemplateImpl;
import io.seata.mongo.tx.mongo.config.ProxyMongoTransactionManager;
import io.seata.mongo.tx.support.MongoSeataRmHelper;
import io.seata.mongo.tx.support.MongoUndoLogImageTemplate;
import io.seata.mongo.tx.support.MongoUndoLogImageTemplateImpl;
import io.seata.spring.boot.autoconfigure.StarterConstants;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @description: SeataMongo自动装配
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/6/10 21:45
 * @version:v1.0
 */
@ConditionalOnProperty(prefix = StarterConstants.SEATA_PREFIX, name = "enabled", havingValue = "false", matchIfMissing = true)
@ConditionalOnBean({MongoTemplate.class})
@Configuration
@ComponentScan(basePackages = "io.seata.mongo.tx")
public class SeataMongoAutoConfiguration {

    @Bean(name = PlainMongoTransactionManager.BEAN_NAME)
    public PlainMongoTransactionManager plainMongoTransactionManager(MongoDatabaseFactory dbFactory, DataSource dataSource) {
        return new PlainMongoTransactionManager(dbFactory, dataSource);
    }

    @Bean(name = ProxyMongoTransactionManager.BEAN_NAME)
    public ProxyMongoTransactionManager proxyMongoTransactionManager(MongoDatabaseFactory dbFactory) {
        return new ProxyMongoTransactionManager(dbFactory);
    }

    @Bean
    public ProxyMongoTemplateImpl proxyMongoTemplate(MongoTemplate mongoTemplate, MongoUndoLogImageTemplate mongoUndoLogImageTemplate) {
        return new ProxyMongoTemplateImpl(mongoTemplate, mongoUndoLogImageTemplate);
    }

    @Bean
    public MongoUndoLogImageTemplate mongoUndoLogImageTemplate(MongoTemplate mongoTemplate, MongoSeataRmHelper mongoSeataRmHelper) {
        return new MongoUndoLogImageTemplateImpl(mongoTemplate, mongoSeataRmHelper);
    }

    @Bean
    public MongoUndoLogOpeService mongoUndoLogOpeService(MongoTemplate mongoTemplate) {
        return new MongoUndoLogOpeService(mongoTemplate);
    }
}
