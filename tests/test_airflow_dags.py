"""Unit tests for Airflow DAGs — structure, schedules, tasks, dependencies."""

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add airflow/dags to sys.path so DAG modules can be imported
DAGS_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


# ---------------------------------------------------------------------------
# Fixtures: import DAG objects
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def housekeeping_dag():
    mod = importlib.import_module("daily_housekeeping")
    return mod.daily_housekeeping()


@pytest.fixture(scope="module")
def backfill_dag():
    mod = importlib.import_module("backfill_missed")
    return mod.backfill_missed()


@pytest.fixture(scope="module")
def quality_dag():
    mod = importlib.import_module("weekly_quality_report")
    return mod.weekly_quality_report()


@pytest.fixture(scope="module")
def health_dag():
    mod = importlib.import_module("health_check")
    return mod.health_check()


ALL_DAG_IDS = [
    "daily_housekeeping",
    "backfill_missed",
    "weekly_quality_report",
    "health_check",
]


# ---------------------------------------------------------------------------
# DAG load / import tests
# ---------------------------------------------------------------------------

class TestDagLoad:
    """Verify all 4 DAGs can be imported without errors."""

    @pytest.mark.parametrize("dag_file", [
        "daily_housekeeping",
        "backfill_missed",
        "weekly_quality_report",
        "health_check",
    ])
    def test_dag_importable(self, dag_file):
        mod = importlib.import_module(dag_file)
        assert mod is not None


# ---------------------------------------------------------------------------
# Schedule tests
# ---------------------------------------------------------------------------

class TestDagSchedules:
    """Verify cron schedules for each DAG."""

    def test_housekeeping_schedule(self, housekeeping_dag):
        assert housekeeping_dag.schedule_interval == "0 2 * * *"

    def test_backfill_schedule(self, backfill_dag):
        assert backfill_dag.schedule_interval == "0 6 * * *"

    def test_quality_report_schedule(self, quality_dag):
        assert quality_dag.schedule_interval == "0 9 * * 1"

    def test_health_check_schedule(self, health_dag):
        assert health_dag.schedule_interval == "*/15 * * * *"


# ---------------------------------------------------------------------------
# Catchup tests
# ---------------------------------------------------------------------------

class TestDagCatchup:
    """All DAGs should have catchup=False."""

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_catchup_disabled(self, dag_id):
        mod = importlib.import_module(dag_id)
        dag_obj = getattr(mod, dag_id)()
        assert dag_obj.catchup is False


# ---------------------------------------------------------------------------
# Tag tests
# ---------------------------------------------------------------------------

class TestDagTags:
    """Verify DAG tags."""

    def test_housekeeping_tags(self, housekeeping_dag):
        assert "housekeeping" in housekeeping_dag.tags
        assert "weaviate" in housekeeping_dag.tags

    def test_backfill_tags(self, backfill_dag):
        assert "backfill" in backfill_dag.tags
        assert "guardian" in backfill_dag.tags

    def test_quality_tags(self, quality_dag):
        assert "quality" in quality_dag.tags
        assert "reporting" in quality_dag.tags

    def test_health_tags(self, health_dag):
        assert "health" in health_dag.tags
        assert "monitoring" in health_dag.tags


# ---------------------------------------------------------------------------
# Task count & dependency tests
# ---------------------------------------------------------------------------

class TestDagTasks:
    """Verify task counts and dependency chains."""

    def test_housekeeping_task_count(self, housekeeping_dag):
        assert len(housekeeping_dag.tasks) == 2

    def test_housekeeping_dependencies(self, housekeeping_dag):
        task_ids = {t.task_id for t in housekeeping_dag.tasks}
        assert "prune_old_articles" in task_ids
        assert "log_prune_stats" in task_ids
        # prune → log
        prune = housekeeping_dag.get_task("prune_old_articles")
        assert "log_prune_stats" in {t.task_id for t in prune.downstream_list}

    def test_backfill_task_count(self, backfill_dag):
        assert len(backfill_dag.tasks) == 3

    def test_backfill_dependencies(self, backfill_dag):
        task_ids = {t.task_id for t in backfill_dag.tasks}
        assert "check_guardian_gaps" in task_ids
        assert "fetch_missing_articles" in task_ids
        assert "publish_to_kafka" in task_ids
        # check → fetch → publish
        check = backfill_dag.get_task("check_guardian_gaps")
        assert "fetch_missing_articles" in {t.task_id for t in check.downstream_list}
        fetch = backfill_dag.get_task("fetch_missing_articles")
        assert "publish_to_kafka" in {t.task_id for t in fetch.downstream_list}

    def test_quality_report_task_count(self, quality_dag):
        assert len(quality_dag.tasks) == 4

    def test_quality_report_dependencies(self, quality_dag):
        task_ids = {t.task_id for t in quality_dag.tasks}
        assert "count_validated" in task_ids
        assert "count_dead_letters" in task_ids
        assert "compute_pass_rate" in task_ids
        assert "log_report" in task_ids
        # [count_validated, count_dead_letters] → compute → log
        compute = quality_dag.get_task("compute_pass_rate")
        upstream_ids = {t.task_id for t in compute.upstream_list}
        assert "count_validated" in upstream_ids
        assert "count_dead_letters" in upstream_ids

    def test_health_check_task_count(self, health_dag):
        assert len(health_dag.tasks) == 4

    def test_health_check_dependencies(self, health_dag):
        task_ids = {t.task_id for t in health_dag.tasks}
        assert "check_kafka" in task_ids
        assert "check_weaviate" in task_ids
        assert "check_ollama" in task_ids
        assert "alert_on_failure" in task_ids
        # [kafka, weaviate, ollama] → alert
        alert = health_dag.get_task("alert_on_failure")
        upstream_ids = {t.task_id for t in alert.upstream_list}
        assert "check_kafka" in upstream_ids
        assert "check_weaviate" in upstream_ids
        assert "check_ollama" in upstream_ids


# ---------------------------------------------------------------------------
# Owner test
# ---------------------------------------------------------------------------

class TestDagOwner:
    """All tasks should have owner=newslens."""

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_owner(self, dag_id):
        mod = importlib.import_module(dag_id)
        dag_obj = getattr(mod, dag_id)()
        for task in dag_obj.tasks:
            assert task.owner == "newslens"
