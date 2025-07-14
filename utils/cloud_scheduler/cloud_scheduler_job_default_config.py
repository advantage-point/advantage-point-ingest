cloud_scheduler_job_default_config = {
    "http_method": "POST",
    "headers": {
        "User-Agent": "Google-Cloud-Scheduler"
    },
    "time_zone": "Etc/UTC",
    "retry_config": {
        "max_retry_duration": "0s",        # disable retry by default
        "min_backoff_duration": "5s",
        "max_backoff_duration": "3600s",
        "max_doublings": 5
    },
    "attempt_deadline": "180s",
}