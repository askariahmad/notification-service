package com.devops.notification.service;

import com.devops.notification.model.Notification;
import com.devops.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notificationRepository;
    
    // Manage sinks per tenant to stream events
    private final Map<String, Sinks.Many<Notification>> tenantSinks = new ConcurrentHashMap<>();

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    private Sinks.Many<Notification> getSinkForTenant(String tenantId) {
        return tenantSinks.computeIfAbsent(tenantId, t -> Sinks.many().multicast().onBackpressureBuffer());
    }

    public Mono<Notification> createNotification(String tenantId, Notification notification) {
        notification.setTenantId(tenantId);
        notification.setTimestamp(new Date());
        notification.setRead(false);
        
        return notificationRepository.save(notification)
                .doOnSuccess(saved -> {
                    log.info("Created notification for tenant {}: {}", tenantId, saved.getTitle());
                    // Emit event to SSE stream
                    getSinkForTenant(tenantId).tryEmitNext(saved);
                });
    }

    public Flux<Notification> getNotificationStream(String tenantId) {
        log.info("Client connected to notification stream for tenant {}", tenantId);
        return getSinkForTenant(tenantId).asFlux();
    }

    public Flux<Notification> getRecentNotifications(String tenantId) {
        return notificationRepository.findByTenantIdOrderByTimestampDesc(tenantId).take(50);
    }
    
    public Mono<Notification> markAsRead(String id) {
        return notificationRepository.findById(id)
                .flatMap(notif -> {
                    notif.setRead(true);
                    return notificationRepository.save(notif);
                });
    }
}
