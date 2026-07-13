package com.devops.notification.controller;

import com.devops.notification.model.Notification;
import com.devops.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @PostMapping
    public Mono<ResponseEntity<Notification>> createNotification(@RequestHeader(value = "X-Tenant-Id", required = false) String tenantId,
                                                                 @RequestBody Notification notification) {
        if (tenantId == null) return Mono.just(ResponseEntity.badRequest().build());
        return notificationService.createNotification(tenantId, notification)
                .map(ResponseEntity::ok);
    }

    @GetMapping
    public Flux<Notification> getRecentNotifications(@RequestHeader(value = "X-Tenant-Id", required = false) String tenantId) {
        if (tenantId == null) return Flux.empty();
        return notificationService.getRecentNotifications(tenantId);
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Notification> streamNotifications(@RequestHeader(value = "X-Tenant-Id", required = false) String tenantId) {
        if (tenantId == null) return Flux.empty();
        return notificationService.getNotificationStream(tenantId);
    }

    @PatchMapping("/{id}/read")
    public Mono<ResponseEntity<Notification>> markAsRead(@PathVariable String id) {
        return notificationService.markAsRead(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
