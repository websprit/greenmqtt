use metrics::{counter, gauge, histogram};

#[allow(dead_code)]
pub(crate) struct BrokerMetrics;

#[allow(dead_code)]
impl BrokerMetrics {
    pub(crate) fn record_connect(tenant_id: &str) {
        counter!("mqtt_connect_total", "tenant_id" => tenant_id.to_string()).increment(1);
    }

    pub(crate) fn record_disconnect(tenant_id: &str) {
        counter!("mqtt_disconnect_total", "tenant_id" => tenant_id.to_string()).increment(1);
    }

    pub(crate) fn record_subscribe(tenant_id: &str) {
        counter!("mqtt_subscribe_total", "tenant_id" => tenant_id.to_string()).increment(1);
    }

    pub(crate) fn record_unsubscribe(tenant_id: &str) {
        counter!("mqtt_unsubscribe_total", "tenant_id" => tenant_id.to_string()).increment(1);
    }

    pub(crate) fn set_active_connections(tenant_id: &str, count: usize) {
        gauge!("mqtt_active_connections", "tenant_id" => tenant_id.to_string()).set(count as f64);
    }

    pub(crate) fn record_publish_ingress(tenant_id: &str, qos: u8, payload_bytes: usize) {
        let qos = qos.to_string();
        counter!(
            "mqtt_publish_count",
            "tenant_id" => tenant_id.to_string(),
            "qos" => qos.clone()
        )
        .increment(1);
        counter!(
            "mqtt_publish_ingress_bytes",
            "tenant_id" => tenant_id.to_string(),
            "qos" => qos
        )
        .increment(payload_bytes as u64);
    }

    pub(crate) fn record_publish_egress(tenant_id: &str, qos: u8, payload_bytes: usize) {
        counter!(
            "mqtt_publish_egress_bytes",
            "tenant_id" => tenant_id.to_string(),
            "qos" => qos.to_string()
        )
        .increment(payload_bytes as u64);
    }

    pub(crate) fn record_qos_latency(qos: u8, latency_seconds: f64) {
        match qos {
            1 => histogram!("mqtt_qos1_latency_seconds").record(latency_seconds),
            2 => histogram!("mqtt_qos2_latency_seconds").record(latency_seconds),
            _ => {}
        }
    }
}
