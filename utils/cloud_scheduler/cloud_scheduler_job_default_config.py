cloud_scheduler_job_default_config = {
    "http_method": "POST",
    "headers": {
        "User-Agent": "Google-Cloud-Scheduler"
    },
    "time_zone": "Etc/UTC",
    "retry_config": {
        "max_retry_duration_seconds": 0,       # disable retry
        "min_backoff_duration_seconds": 5,
        "max_backoff_duration_seconds": 3600,
        "max_doublings": 5
    },
    "attempt_deadline_seconds": 180
}