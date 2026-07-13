# Notification Service

## Low-Level Design (LLD) & In-Depth Overview

The **Notification Service** handles outbound communication to alert engineering teams of anomalies.

### Key Responsibilities
1. **Event Consumption**: Consumes events from the `incident-notifications` Kafka topic.
2. **Tenant Configuration Lookup**: Queries the `config-service` to determine the tenant's preferred notification methods (Webhook, Slack, Email).
3. **Dispatch**: Formats the incident data and executes the HTTP requests to external Webhooks or Slack channels.

### How to Interact
- **Port**: `8088` (Internal Docker port)
- **Kafka Topic**: Consumes `incident-notifications`.
- **Dependencies**: Relies on `config-service` to retrieve Webhook URLs.
