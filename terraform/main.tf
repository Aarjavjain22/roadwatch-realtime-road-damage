terraform {
  required_providers {
    grafana = {
      source  = "grafana/grafana"
      version = "~> 3.0"
    }
  }
}

# Provisions Grafana dashboards + folder AS CODE against the running Grafana
# (brought up by docker-compose). This is the "dashboards on Terraform" story:
#   docker compose up -d   # infra
#   terraform init && terraform apply   # dashboards/alerts as code
provider "grafana" {
  url  = var.grafana_url
  auth = var.grafana_auth
}

resource "grafana_folder" "roadwatch" {
  title = "RoadWatch"
}

resource "grafana_dashboard" "observability" {
  folder      = grafana_folder.roadwatch.id
  config_json = file("${path.module}/../observability/grafana/dashboards/roadwatch.json")
  overwrite   = true
}
