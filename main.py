from src.driver import DataSyncClient
from apscheduler.schedulers.blocking import BlockingScheduler
from src.config import FinancialQueries


    
def job():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_all_yesterday()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(job, 'cron', hour=8, minute=0)
    scheduler.start()

