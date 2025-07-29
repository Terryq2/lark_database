from apscheduler.schedulers.blocking import BlockingScheduler
from src.driver import DataSyncClient


def job_for_cinema_tickets():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_most_recent_data('C01', syncer.config.get_name('C01'))

def job_for_others():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_all_yesterday()

if __name__ == "__main__":
    syncer = DataSyncClient(".env", "config.json")
    syncer.lark_client._get_user_ids("总部")


    # scheduler = BlockingScheduler()
    # scheduler.add_job(job_for_cinema_tickets, 'cron', hour=8, minute=0)
    # scheduler.add_job(job_for_others, 'cron', hour=8, minute=30)
    # scheduler.start()

