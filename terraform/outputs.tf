output "dashboard_url" {
  description = "Open this to view the provisioned observability dashboard"
  value       = "${var.grafana_url}/d/${grafana_dashboard.observability.uid}"
}
