"""Unit tests for Slack Alerting & Notification Module."""

import unittest
from unittest.mock import MagicMock, patch

from src.alerts.slack import (
    get_slack_config,
    is_slack_configured,
    send_data_quality_alert,
    send_infrastructure_alert,
    send_pipeline_start_alert,
    send_job_start_alert,
    send_pipeline_summary_alert,
    send_sla_miss_alert,
    send_slack_alert,
    send_task_failure_alert,
    send_task_retry_alert,
    test_slack_connection,
)


class TestSlackAlerts(unittest.TestCase):
    """Test suite for the Slack Alerting subsystem."""

    def test_get_slack_config(self):
        """Verify configuration reading with defaults."""
        cfg = get_slack_config()
        self.assertIn("webhook_url", cfg)
        self.assertIn("channel", cfg)
        self.assertIn("enabled", cfg)
        self.assertIn("alert_on_failure", cfg)
        self.assertIn("alert_on_retry", cfg)
        self.assertIn("alert_on_success", cfg)
        self.assertIn("alert_on_quarantine", cfg)
        self.assertIn("alert_on_sla_miss", cfg)

    def test_dry_run_when_webhook_unconfigured(self):
        """Verify that missing webhook URL results in a safe dry-run without errors."""
        ok, msg = send_slack_alert(
            title="Test Dry Run",
            message="Dry run test message",
            level="INFO",
            custom_webhook="",
        )
        self.assertTrue(ok)
        self.assertIn("DRY_RUN", msg)

    @patch("requests.post")
    def test_send_slack_alert_live_success(self, mock_post):
        """Verify HTTP POST is properly formatted and dispatched when webhook is provided."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_post.return_value = mock_response

        webhook = "https://hooks.slack.com/services/T00000000/B00000000/REALTOKEN123"
        ok, msg = send_slack_alert(
            title="Live Alert Test",
            message="Testing Slack attachment formatting",
            level="CRITICAL",
            fields={"Task": "job1_extract", "Status": "FAILED"},
            custom_webhook=webhook,
        )

        self.assertTrue(ok)
        self.assertEqual(msg, "SUCCESS")
        self.assertTrue(mock_post.called)
        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["json"]["attachments"][0]["color"], "#E01E5A")
        self.assertEqual(call_kwargs["json"]["attachments"][0]["title"], "Live Alert Test")

    @patch("requests.post")
    def test_send_task_failure_alert(self, mock_post):
        """Verify task failure alert formats context correctly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        context = {
            "task_id": "job3_clean_data",
            "dag_id": "nyc_taxi_batch_pipeline",
            "execution_date": "2023-01-01T00:00:00Z",
            "exception": "OutOfMemoryError: Container killed",
            "try_number": 3,
            "max_tries": 3,
        }

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_task_failure_alert(context)
            self.assertTrue(ok)
            self.assertEqual(msg, "SUCCESS")

    @patch("requests.post")
    def test_send_task_retry_alert(self, mock_post):
        """Verify retry alert formatting."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        context = {
            "task_id": "job1_extract_to_raw",
            "dag_id": "nyc_taxi_batch_pipeline",
            "try_number": 1,
            "max_tries": 3,
            "exception": "ConnectionTimeout",
        }

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_task_retry_alert(context)
            self.assertTrue(ok)

    @patch("requests.post")
    def test_send_data_quality_alert(self, mock_post):
        """Verify data quality quarantine alert triggers with rate calculation."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 2.0,
            }
            ok, msg = send_data_quality_alert(
                batch_run_id="run-123",
                source_file="yellow_tripdata_2023-01.parquet",
                quarantined_count=5000,
                total_count=100000,
                rejection_reasons={"Negative Fare": 2500, "Outlier Duration": 2500},
            )
            self.assertTrue(ok)

    @patch("requests.post")
    def test_send_pipeline_summary_alert(self, mock_post):
        """Verify pipeline execution summary alert."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        summary = {
            "job_id": "test-job-999",
            "target_months": ["2023-01"],
            "total_rows_extracted": 3000000,
            "total_rows_cleansed": 2950000,
            "total_rows_fact_loaded": 2950000,
            "total_rows_master_loaded": 2950000,
            "total_rows_quarantined": 50000,
            "total_duration_sec": 42.5,
            "overall_throughput_rows_sec": 69411.7,
            "peak_memory_mb": 115.0,
        }

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_pipeline_summary_alert(summary)
            self.assertTrue(ok)

    @patch("requests.post")
    def test_send_infrastructure_alert(self, mock_post):
        """Verify service offline alerts."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_infrastructure_alert("PostgreSQL Warehouse", "OFFLINE", 5432, "Connection refused")
            self.assertTrue(ok)

    @patch("requests.post")
    def test_send_pipeline_start_alert(self, mock_post):
        """Verify pipeline start alerts are dispatched cleanly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_start": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_pipeline_start_alert(
                job_id="test_pipeline_run",
                target_months=["2023-01", "2023-02"],
                timeframe=("2023-01-01", "2023-02-28"),
                execution_engine="Apache Airflow",
            )
            self.assertTrue(ok)
            self.assertEqual(msg, "SUCCESS")

    @patch("requests.post")
    def test_send_job_start_alert(self, mock_post):
        """Verify individual month job start alerts are dispatched cleanly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        with patch("src.alerts.slack.get_slack_config") as mock_cfg:
            mock_cfg.return_value = {
                "webhook_url": "https://hooks.slack.com/services/T/B/X",
                "channel": "#alerts",
                "enabled": True,
                "alert_on_start": True,
                "alert_on_failure": True,
                "alert_on_retry": True,
                "alert_on_success": True,
                "alert_on_sla_miss": True,
                "alert_on_quarantine": True,
                "quarantine_threshold": 5.0,
            }
            ok, msg = send_job_start_alert(
                month="2023-01",
                file_name="yellow_tripdata_2023-01.parquet",
                timeframe=("2023-01-01", "2023-01-31"),
            )
            self.assertTrue(ok)
            self.assertEqual(msg, "SUCCESS")


if __name__ == "__main__":
    unittest.main()
