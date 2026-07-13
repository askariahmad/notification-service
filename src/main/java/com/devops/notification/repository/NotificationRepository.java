package com.devops.notification.repository;

import com.devops.notification.model.Notification;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface NotificationRepository extends ReactiveMongoRepository<Notification, String> {
    Flux<Notification> findByTenantIdOrderByTimestampDesc(String tenantId);
    Flux<Notification> findByTenantIdAndIsReadFalseOrderByTimestampDesc(String tenantId);
}
