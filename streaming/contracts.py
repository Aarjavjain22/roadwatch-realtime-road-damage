"""Dependency-free RoadWatch event-contract utilities."""
from __future__ import annotations

EVENT_VERSION = "1.0"
CLASS_NAMES = ["Longitudinal", "Transverse", "Alligator", "Pothole"]
REQUIRED_FIELDS = [
    "event_version", "event_id", "frame_id", "trace_id",
    "damage_type", "confidence", "road_segment",
]


def severity_from_area(area_ratio: float, conf: float) -> str:
    if area_ratio > 0.03 or conf > 0.7:
        return "High"
    if area_ratio > 0.01 or conf > 0.45:
        return "Medium"
    return "Low"


def validation_error(event: dict) -> str | None:
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] in (None, ""):
            return "schema_invalid"

    if event.get("event_version") != EVENT_VERSION:
        return "unsupported_event_version"

    confidence = event.get("confidence")
    if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        return "confidence_out_of_range"

    if event.get("damage_type") not in CLASS_NAMES:
        return "unknown_damage_type"

    return None
