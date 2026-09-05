# Operations Runbook

## Start the platform

```bash
cp .env.example .env
docker compose build
docker compose up -d
docker compose --profile spark up -d spark
```

## Service health

```bash
docker compose ps
docker compose logs --tail=100 kafka producer yolo-consumer analytics-consumer
```

Operational endpoints:

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka: `localhost:29092`
- PostgreSQL: `localhost:5432`

## Kafka topic inventory

```bash
docker exec roadwatch-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe
```

Expected topics:

- `raw_frames` — 6 partitions
- `road_damage_events` — 6 partitions
- `dead_letter` — 3 partitions

## Consumer-group state

```bash
docker exec roadwatch-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe
```

Key groups:

- `roadwatch-inference`
- `roadwatch-persistence`

## Event count

```bash
docker exec -it roadwatch-postgres \
  psql -U roadwatch -d roadwatch \
  -c "SELECT COUNT(*) FROM detection_events;"
```

## Data-quality / distribution check

```bash
docker exec -it roadwatch-postgres \
  psql -U roadwatch -d roadwatch \
  -c "SELECT damage_type, severity, COUNT(*) FROM detection_events GROUP BY 1,2 ORDER BY 3 DESC;"
```

## Recent events

```bash
docker exec -it roadwatch-postgres \
  psql -U roadwatch -d roadwatch \
  -c "SELECT event_time, road_segment, damage_type, confidence, severity FROM detection_events ORDER BY event_time DESC LIMIT 20;"
```

## Recovery drill: inference backlog

```bash
docker compose stop yolo-consumer
# Producer remains active; roadwatch-inference lag increases.

docker compose start yolo-consumer
# Worker resumes from the last committed offset; backlog drains.
```

## Recovery drill: persistence outage

```bash
docker compose stop postgres
# analytics-consumer cannot complete DB side effects and therefore does not commit the current event.

docker compose start postgres
docker compose restart analytics-consumer
# Uncommitted Kafka events are eligible for replay.
```

## DLQ inspection

```bash
docker exec roadwatch-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dead_letter \
  --from-beginning \
  --max-messages 20
```

## Prometheus targets

Open `http://localhost:9090/targets` and verify that these jobs are healthy:

- `producer`
- `yolo-consumer`
- `analytics-consumer`
- `kafka-exporter`

## Reprovision Grafana with Terraform

```bash
cd terraform
terraform init
terraform apply
```

## Reset all local state

```bash
docker compose --profile spark down -v
```

This removes PostgreSQL and Spark checkpoint volumes and returns the local environment to a clean state.
