from lib.utils import BaseClient
from pipeline.jobs import CleaningJob, FillMongoDBJob, ListingJob, ReportJob, VolumeJob


class JobManager(BaseClient):
    def __init__(self):
        self.parser = self._init_args()
        self.config = self._init_config()

    def run(self):
        args = self.parser.parse_args()
        if args.volume:
            job = VolumeJob(self.config)
            job.run(args.volume_num)
        elif args.listing:
            job = ListingJob(self.config)
            job.run()
        elif args.cleaning:
            job = CleaningJob(self.config)
            job.run()
        elif args.report:
            job = ReportJob(self.config)
            job.run(date=args.date, cat=args.report_cat, num=args.report_num)
        elif args.fill_mongodb:
            job = FillMongoDBJob(self.config)
            job.run()


if __name__ == "__main__":
    job_manager = JobManager()
    job_manager.run()
