variable "grafana_url" {
  description = "URL of the running Grafana instance"
  type        = string
  default     = "http://localhost:3000"
}

variable "grafana_auth" {
  description = "Grafana admin auth as user:password (or an API token)"
  type        = string
  default     = "admin:admin"
  sensitive   = true
}
