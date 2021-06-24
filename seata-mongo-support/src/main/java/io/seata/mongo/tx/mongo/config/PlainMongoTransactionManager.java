package io.seata.mongo.tx.mongo.config;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

/**
 * @description: 不生成undolog的mongo事务管理器
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/12 16:01
 * @version:v1.0
 */
@Slf4j
public class PlainMongoTransactionManager extends MongoTransactionManager {

    public static final String BEAN_NAME = "plainTransactionManager";

    public PlainMongoTransactionManager(MongoDatabaseFactory dbFactory, DataSource dataSource) {
        super(dbFactory);
    }
}
