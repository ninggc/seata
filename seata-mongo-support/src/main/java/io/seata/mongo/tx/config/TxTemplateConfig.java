package io.seata.mongo.tx.config;

import io.seata.mongo.tx.mongo.config.PlainMongoTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @description: db事务模板
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/18 14:34
 * @version:v1.0
 */
@Configuration(proxyBeanMethods = false)
public class TxTemplateConfig {

    /**
     * mysql事务模板
     * @see org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration.TransactionTemplateConfiguration#transactionTemplate(org.springframework.transaction.PlatformTransactionManager)
     * @param transactionManager
     * @return
     */
    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    /**
     * 不生成undolog的mongo事务模板
     * @param plainMongoTransactionManager
     * @return
     */
    @Bean
    public TransactionTemplate plainMongoTransactionTemplate(PlainMongoTransactionManager plainMongoTransactionManager) {
        return new TransactionTemplate(plainMongoTransactionManager);
    }
}
