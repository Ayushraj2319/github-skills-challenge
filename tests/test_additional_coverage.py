import json
import subprocess
import sys

import pytest

from src.aiops_pipeline import load_data, run_pipeline
from src.anomaly_detector import AnomalyDetector
from src.calculations import area_of_circle, get_nth_fibonacci
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_load_data_reads_value_from_disk(tmp_path):
    file_path = tmp_path / "records.json"
    file_path.write_text(json.dumps([{"service": "billing"}]), encoding="utf-8")

    assert load_data(str(file_path)) == [{"service": "billing"}]


def test_anomaly_detector_flags_warning_record():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "checkout-service",
        "response_time_ms": 100,
        "cpu_percent": 40,
        "memory_percent": 50,
        "log_level": "WARNING",
        "message": "warning message"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"
    assert "Error log detected" in event["reasons"]


def test_run_pipeline_returns_expected_summary_for_anomalies(tmp_path):
    file_path = tmp_path / "service_data.json"
    file_path.write_text(json.dumps([
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "order-service",
            "response_time_ms": 100,
            "cpu_percent": 35,
            "memory_percent": 40,
            "log_level": "INFO",
            "message": "ok"
        },
        {
            "timestamp": "2026-09-20T10:01:00",
            "service": "order-service",
            "response_time_ms": 600,
            "cpu_percent": 90,
            "memory_percent": 85,
            "log_level": "ERROR",
            "message": "timeout"
        }
    ]), encoding="utf-8")

    result = run_pipeline(str(file_path))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["anomalies_detected"][0]["service"] == "order-service"
    assert result["events_consumed"] == []


def test_event_topic_and_producer_consumer_workflow():
    topic = EventTopic("alerts")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    assert producer.publish({"type": "ANOMALY", "service": "auth-service"}) is True
    assert len(topic.get_messages()) == 1
    assert consumer.consume() == [{"type": "ANOMALY", "service": "auth-service"}]

    topic.clear()
    assert topic.get_messages() == []


def test_calculation_helpers_cover_error_paths_and_fibonacci_value():
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)

    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)

    assert get_nth_fibonacci(10) == 55


def test_main_entrypoint_runs_pipeline_script():
    result = subprocess.run(
        [sys.executable, "src/aiops_pipeline.py"],
        cwd=".",
        env={**dict(__import__("os").environ), "PYTHONPATH": "."},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "AIOps Pipeline Result" in result.stdout
