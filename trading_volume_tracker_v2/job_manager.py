import argparse
import os
import json
from pipeline.jobs import VolumeJob, ListingJob, CleaningJob
from lib.utils import BaseClient


class JobManager(BaseClient):
    
    def __init__(self):
        self.parser = self._init_args()
        self.config = self._init_config()
    
    def run(self):
        args = self.parser.parse_args()
        if args.volume:
            job = VolumeJob(self.config)
            job.run(args.num)
        elif args.listing:
            job = ListingJob(self.config)
            job.run()
        elif args.cleaning:
            job = CleaningJob(self.config)
            job.run()
            

if __name__ == "__main__":
    job_manager = JobManager()
    job_manager.run()
