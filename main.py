"""
Main entry point
"""
from datetime import datetime, date
import json
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from src.driver import DataSyncClient
from utility.helpers import compose_table_name
from src.config import FinancialQueries

def job(syncer: DataSyncClient):
    _job_for_others(syncer)
    _job_for_cinema_ticket_day(syncer)
    _message_after_job(syncer)

def _job_for_cinema_tickets_hourly(syncer: DataSyncClient):
    today = date.today().strftime("%Y-%m-%d")
    table_name = compose_table_name(syncer.config.get_name('C01'))

    list_of_ids = syncer.lark_client.get_table_records_id_at_dates(table_name, [today])
    syncer.lark_client.delete_records_by_id(table_name, list_of_ids)
    query_data_today = FinancialQueries('C01', 'day', today)
    syncer.upload_data(query_data_today, table_name)
    _message_after_tickets_job(syncer)

def _job_for_cinema_ticket_day(syncer: DataSyncClient):
    table_name = compose_table_name(syncer.config.get_name('C01'))
    list_of_ids = syncer.lark_client.get_table_records_id_at_head_date(table_name, syncer._get_primary_timestamp_column_name('C01'))
    syncer.lark_client.delete_records_by_id(table_name, list_of_ids)

def _job_for_others(syncer: DataSyncClient):
    syncer.sync_all_yesterday()
    syncer.sync_screening_data()


def _message_after_tickets_job(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SYNCED TICKETS DATA</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

def _message_after_job(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SYNCED OTHERS</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

def _message_init(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SERVER INITIALIZING</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

if __name__ == "__main__":
    global_syncer = DataSyncClient(".env", "config.json")
    try:
        _message_init(global_syncer)
        scheduler = BlockingScheduler()

        scheduler.add_job(job, 'cron', hour=8, minute=0, args=[global_syncer])
        scheduler.add_job(
            _job_for_cinema_tickets_hourly,
            'cron',
            hour='*',
            minute=0,
            args=[global_syncer]
        )
        scheduler.start()
    except Exception as e:
        os._exit(1)

        

