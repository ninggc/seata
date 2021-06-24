package io.seata.mongo.tx.mongo.component;

import javax.annotation.Resource;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @description:
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/10 13:52
 * @version:v1.0
 */
@Component
public class SpringSeataTxSupportHelper implements ApplicationContextAware {

    private static ApplicationContext context;

    @Resource
    private MongoUndoLogService mongoUndoLogService;

    private static SpringSeataTxSupportHelper getInstance() {
        if (context == null) {
            throw new IllegalStateException("context未初始化");
        }
        return context.getBean(SpringSeataTxSupportHelper.class);
    }

    private static <T> T getBean(Class<T> clazz) {
        return SpringSeataTxSupportHelper.context.getBean(clazz);
    }

    public static MongoUndoLogService getMongoUndoLogService() {
        return getInstance().mongoUndoLogService;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringSeataTxSupportHelper.context = applicationContext;
    }
}
