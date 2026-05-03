from typing import Dict
from models import JobDefinition


class JobRegistry:

    _jobs: Dict[str, JobDefinition] = {}

    @classmethod
    def register(cls, job: JobDefinition):
        if job.jobName in cls._jobs:
            raise ValueError(f"Job already registered: {job.jobName}")

        cls._jobs[job.jobName] = job

    @classmethod
    def get_jobs(cls):
        return cls._jobs

    @classmethod
    def get_by_dag(cls, dag_name: str):
        return {
            k: v for k, v in cls._jobs.items()
            if v.dagName == dag_name
        }