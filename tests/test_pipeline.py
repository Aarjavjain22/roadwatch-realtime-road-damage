import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))

from contracts import EVENT_VERSION, severity_from_area, validation_error  # noqa: E402


def base_event():
    return {
        "event_version": EVENT_VERSION,
        "event_id": "evt_frame_1_000",
        "trace_id": "frame_1",
        "frame_id": "frame_1",
        "damage_type": "Pothole",
        "confidence": 0.7,
        "road_segment": "segment_01",
    }


def test_severity_levels():
    assert severity_from_area(0.05, 0.9) == "High"
    assert severity_from_area(0.02, 0.5) == "Medium"
    assert severity_from_area(0.001, 0.3) == "Low"


def test_valid_event_passes():
    assert validation_error(base_event()) is None


def test_missing_field_rejected():
    event = base_event()
    event.pop("road_segment")
    assert validation_error(event) == "schema_invalid"


def test_confidence_out_of_range_rejected():
    event = base_event()
    event["confidence"] = 1.7
    assert validation_error(event) == "confidence_out_of_range"


def test_unsupported_version_rejected():
    event = base_event()
    event["event_version"] = "2.0"
    assert validation_error(event) == "unsupported_event_version"


def test_unknown_damage_type_rejected():
    event = base_event()
    event["damage_type"] = "Unknown"
    assert validation_error(event) == "unknown_damage_type"
