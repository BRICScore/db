from celery import Celery
from celery.schedules import crontab

worker_app = Celery("Background Tasks", broker="redis://redis:6379/0", backend="redis://redis:6379/1")
worker_app.conf.task_serializer = "json"
worker_app.conf.accept_content = ["json"]
worker_app.conf.result_serializer = "json"
worker_app.autodiscover_tasks(["src.tasks"])
worker_app.conf.beat_schedule = {
    "mongodb-backup-every-night": {
        "task": "src.tasks.backup_mongodb_and_files",
        "schedule": crontab(hour=2, minute=0),
    }
}

worker_app.conf.timezone = "UTC"