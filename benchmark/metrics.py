"""
Performance metrics collection and logging for TPC-DI benchmark.
"""

import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class StepMetrics:
    """Metrics for a single ETL step."""
    step_name: str
    start_time: float
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    status: str = "running"  # running, completed, failed
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def finish(self, rows: Optional[int] = None, bytes: Optional[int] = None, 
               metadata: Optional[Dict[str, Any]] = None):
        """Mark step as completed."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.status = "completed"
        if rows is not None:
            self.rows_processed = rows
        if bytes is not None:
            self.bytes_processed = bytes
        if metadata:
            self.metadata = metadata
    
    def fail(self, error_message: str):
        """Mark step as failed."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.status = "failed"
        self.error_message = error_message


@dataclass
class BenchmarkMetrics:
    """Complete metrics for a benchmark run."""
    platform: str
    load_type: str
    scale_factor: int
    batch_id: Optional[int]
    start_time: float
    end_time: Optional[float] = None
    total_duration_seconds: Optional[float] = None
    steps: List[StepMetrics] = None
    summary: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.steps is None:
            self.steps = []
    
    def finish(self):
        """Mark benchmark as completed."""
        self.end_time = time.time()
        self.total_duration_seconds = self.end_time - self.start_time
        
        # Calculate summary
        completed_steps = [s for s in self.steps if s.status == "completed"]
        failed_steps = [s for s in self.steps if s.status == "failed"]
        
        total_rows = sum(s.rows_processed or 0 for s in completed_steps)
        total_bytes = sum(s.bytes_processed or 0 for s in completed_steps)
        total_duration = sum(s.duration_seconds or 0 for s in completed_steps)
        
        self.summary = {
            "total_steps": len(self.steps),
            "completed_steps": len(completed_steps),
            "failed_steps": len(failed_steps),
            "total_rows_processed": total_rows,
            "total_bytes_processed": total_bytes,
            "total_step_duration_seconds": total_duration,
            "throughput_rows_per_second": total_rows / total_duration if total_duration > 0 else 0,
            "throughput_mb_per_second": (total_bytes / (1024 * 1024)) / total_duration if total_duration > 0 else 0,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "platform": self.platform,
            "load_type": self.load_type,
            "scale_factor": self.scale_factor,
            "batch_id": self.batch_id,
            "start_time": self.start_time,
            "start_time_iso": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": self.end_time,
            "end_time_iso": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "total_duration_seconds": self.total_duration_seconds,
            "steps": [asdict(step) for step in self.steps],
            "summary": self.summary,
        }
    
    def save(self, output_path: str):
        """Save metrics to file (JSON). Local paths use pathlib/open; gs:// paths write to temp then upload via gsutil."""
        timestamp = datetime.fromtimestamp(self.start_time).strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_{self.platform}_{self.load_type}_sf{self.scale_factor}_{timestamp}.json"
        if self.batch_id is not None:
            filename = f"metrics_{self.platform}_{self.load_type}_sf{self.scale_factor}_batch{self.batch_id}_{timestamp}.json"

        if output_path.startswith("gs://"):
            # pathlib.Path("gs://bucket/path") turns gs:// into gs:/ (one slash). Build path as string and upload.
            base = output_path.rstrip("/")
            full_gcs_path = f"{base}/{filename}"
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(self.to_dict(), f, indent=2)
                tmp_path = f.name
            try:
                subprocess.run(
                    ["gsutil", "-q", "cp", tmp_path, full_gcs_path],
                    check=True,
                    capture_output=True,
                )
                logger.info(f"Metrics saved to {full_gcs_path}")
                return full_gcs_path
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
        else:
            # Local path (or dbfs:/ on Databricks if mounted)
            output = Path(output_path)
            output.mkdir(parents=True, exist_ok=True)
            filepath = output / filename
            with open(filepath, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Metrics saved to {filepath}")
            return str(filepath)


class MetricsCollector:
    """Context manager for collecting benchmark metrics."""
    
    def __init__(self, config):
        self.config = config
        self.metrics = BenchmarkMetrics(
            platform=config.platform.value,
            load_type=config.load_type.value,
            scale_factor=config.scale_factor,
            batch_id=config.batch_id,
            start_time=time.time(),
        )
        self.current_step: Optional[StepMetrics] = None
    
    def start_step(self, step_name: str) -> StepMetrics:
        """Start a new step."""
        if self.current_step and self.current_step.status == "running":
            logger.warning(f"Previous step '{self.current_step.step_name}' not finished, marking as incomplete")
            self.current_step.fail("Step not properly finished")
        
        self.current_step = StepMetrics(
            step_name=step_name,
            start_time=time.time(),
        )
        self.metrics.steps.append(self.current_step)
        logger.info(f"Started step: {step_name}")
        return self.current_step
    
    def finish_step(self, rows: Optional[int] = None, bytes: Optional[int] = None,
                   metadata: Optional[Dict[str, Any]] = None):
        """Finish current step."""
        if self.current_step:
            self.current_step.finish(rows=rows, bytes=bytes, metadata=metadata)
            logger.info(f"Completed step: {self.current_step.step_name} "
                       f"({self.current_step.duration_seconds:.2f}s, "
                       f"{self.current_step.rows_processed or 0} rows)")
            self.current_step = None
        else:
            logger.warning("No current step to finish")
    
    def fail_step(self, error_message: str):
        """Mark current step as failed."""
        if self.current_step:
            self.current_step.fail(error_message)
            logger.error(f"Failed step: {self.current_step.step_name} - {error_message}")
            self.current_step = None
        else:
            logger.warning("No current step to fail")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finish benchmark and save metrics."""
        if self.current_step and self.current_step.status == "running":
            if exc_type:
                self.fail_step(str(exc_val))
            else:
                self.finish_step()
        
        self.metrics.finish()
        
        if self.config.enable_metrics and self.config.metrics_output_path:
            try:
                self.metrics.save(self.config.metrics_output_path)
            except Exception as e:
                logger.error(f"Failed to save metrics: {e}")
        
        return False  # Don't suppress exceptions
