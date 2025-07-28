from src.driver import DataSyncClient
from apscheduler.schedulers.blocking import BlockingScheduler


    
def today():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_all_today()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(today, 'cron', hour=20, minute=48)
    scheduler.start()

