# Maintainer: GABIT Marco Gantenbein
# Licence: GPLv3

import logging
import os
import re
import time
import warnings
import xmlrpc.client
from datetime import datetime
from urllib.parse import urljoin

import pysftp
import requests
import schedule
from dateutil.relativedelta import relativedelta
from pytz import timezone, UnknownTimeZoneError

warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ENV = os.environ
get_env = ENV.get

CNOPTS = pysftp.CnOpts()
CNOPTS.hostkeys = None


def get_env_or_default(key, default=None, converter=str):
    value = get_env(key)
    if not value:
        return default if default is not None else converter()

    return converter(value)


URL = get_env('ODOO_URL')
MASTER_PWD = get_env('ODOO_MASTER_PWD')
NAME = get_env('ODOO_DB_NAME')
FORMAT = get_env_or_default('ODOO_BACKUP_FORMAT', default="zip").lower()
BACKUP_TIME = get_env_or_default('BACKUP_TIME', default="02:00")
BACKUP_EVERY_HOUR = get_env_or_default('BACKUP_EVERY_HOUR', default=None, converter=int)
DAILY_BACKUP_KEEP = get_env_or_default('DAILY_BACKUP_KEEP', default=30, converter=int)
MONTHLY_BACKUP_KEEP = get_env_or_default('MONTHLY_BACKUP_KEEP', default=12, converter=int)
YEARLY_BACKUP_KEEP = get_env_or_default('YEARLY_BACKUP_KEEP', default=-1, converter=int)
SFTP_HOST = get_env('SFTP_HOST')
SFTP_PORT = get_env_or_default('SFTP_PORT', default=22, converter=int)
SFTP_USER = get_env('SFTP_USER')
SFTP_PASSWORD = get_env('SFTP_PASSWORD')
SFTP_PATH = get_env_or_default('SFTP_PATH', default="/")
TEST_MODE = get_env('TEST_MODE')
TIME_ZONE = get_env_or_default('TZ', default='UTC')

try:
    TZ = timezone(TIME_ZONE)
except UnknownTimeZoneError:
    TZ = timezone('UTC')


class SFTPHandler:
    def __init__(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD,
                                      cnopts=cnopts)

    def upload(self, file_path, file_name):
        self.sftp.put(file_path, os.path.join(SFTP_PATH, file_name))

    def remove(self, file):
        self.sftp.remove(os.path.join(SFTP_PATH, file))

    def list_files(self):
        return self.sftp.listdir(SFTP_PATH)

    def close(self):
        self.sftp.close()


def backup():
    try:
        if (URL and MASTER_PWD and NAME and FORMAT.lower() in ["zip", "dump"] and
                SFTP_HOST and SFTP_USER and SFTP_PASSWORD):
            common = xmlrpc.client.ServerProxy(urljoin(URL, "/xmlrpc/2/common"))
            version = common.version()
            logger.info(f'*** Starting backup process for odoo {version["server_serie"]} with database "{NAME}" on "{URL}" ***')
            handler = SFTPHandler()
            now = datetime.now(tz=TZ)
            backup_file_name = f"odoo{version['server_serie']}-{NAME}-{now.strftime('%Y%m%d-%H%M%S')}.{FORMAT.lower()}"
            backup_file_path = os.path.join(
                './backups', backup_file_name
            )
            _backup_request(backup_file_path)
            _backup_upload(handler, backup_file_path, backup_file_name)
            backups_to_remove = _backup_cleanup(handler, now)
            _remove_backups(handler, backups_to_remove)
            handler.close()
            logger.info(f'*** Finished backup process for odoo {version["server_serie"]} with database "{NAME}" on "{URL}" ***')
        else:
            _backup_help()
    except (xmlrpc.client.ProtocolError, xmlrpc.client.Fault):
        logger.error(
            'Error occurred during connection to odoo. Please check if odoo is reachable under the provided ODOO_URL.')
    except pysftp.exceptions.ConnectionException:
        logger.exception('Exception occurred during connecting to sFTP Server. Please check provided sFTP Credentials')
    except Exception as e:
        logger.exception(f'Exception occurred during backup: {e}')


def _backup_request(backup_file_path):
    try:
        odoo_backup_url = urljoin(URL, '/web/database/backup')
        logger.info(f'Requesting backup on url "{odoo_backup_url}"')
        data = {
            "master_pwd": MASTER_PWD,
            "name": NAME,
            "backup_format": FORMAT,
        }
        response = requests.post(odoo_backup_url, data=data, stream=True)
        if response.status_code == 200:
            with open(backup_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
        else:
            logger.error(f'Backup request failed. Status code: {response.status_code}')
    except requests.RequestException as e:
        logger.exception(f'Exception occurred during backup request: {e}')


def _backup_upload(handler, backup_file_path, backup_file_name):
    try:
        logger.info(f'Uploading backup to {SFTP_HOST}.')
        handler.upload(backup_file_path, backup_file_name)
        os.remove(backup_file_path)
    except Exception as e:
        logger.exception(f'Exception occurred during backup upload: {e}')


def _backup_cleanup(handler, now):
    try:
        logger.info('Cleaning up old backups.')
        backups_to_remove = []

        files = _get_files_with_datetime(handler)

        if BACKUP_EVERY_HOUR:
            backup_time_to_keep = int(BACKUP_TIME.split(":")[0])
            backups_to_remove.extend(
                filter(lambda x: x[0] < now - relativedelta(days=1) and x[0].hour != backup_time_to_keep, files)
            )
        backups_to_remove.extend(
            filter(lambda x: x[0] < now - relativedelta(months=1) and x[0].day != 1, files)
        )

        backups_to_remove.extend(
            filter(lambda x: x[0] < now - relativedelta(years=1) and x[0].month != 1, files)
        )

        if YEARLY_BACKUP_KEEP != -1:
            backups_to_remove.extend(
                filter(lambda x: x[0] < now - relativedelta(years=YEARLY_BACKUP_KEEP), files)
            )

        return backups_to_remove
    except Exception as e:
        logger.exception(f'Exception occurred during backup cleanup: {e}')


def _get_files_with_datetime(handler):
    pattern = re.compile(r'\d{8}-\d{6}')
    files = handler.list_files()
    for file in files:
        match = pattern.search(file)
        if match:
            try:
                yield datetime.strptime(match.group(), '%Y%m%d-%H%M%S').replace(tzinfo=TZ), file
            except ValueError as e:
                logger.error(f'Valuer Error on getting date from file name: {e}')


def _remove_backups(handler, backups_to_remove):
    logger.info(f'Remove {len(backups_to_remove)} backups from sftp server.')
    for file in backups_to_remove:
        handler.remove(file[1])


def _backup_help():
    logger.info("""Usage Environment variables: [options...]
***Required Environment variables***
ODOO_URL, URL odoo server
ODOO_MASTER_PWD, odoo Master password
ODOO_DB_NAME, odoo Database name to backup
SFTP_HOST,  sFTP Server Address
SFTP_USER, sFTP Server User
SFTP_PASSWORD, sFTP Server Password

***Optional Environment variables***
ODOO_BACKUP_FORMAT, Backup format  zip or dump, default=zip
BACKUP_TIME, Start time for Backup or time which daily backup should be preserved, default="02:00"
BACKUP_EVERY_HOUR, Set hours between the backups if hourly backup should be done, default=None
DAILY_BACKUP_KEEP, Set count of daily backups to keep, default=30
MONTHLY_BACKUP_KEEP, Set count of monthly backups to keep, default=12
YEARLY_BACKUP_KEEP, Set count of yearly backups to keep (unlimited=-1), default=-1
SFTP_PORT, Port of sFTP Server, default=22
SFTP_PATH, Directory Path on sFTP Server, default="/"
TEST_MODE, Set TEST_MODE=True to directly execute backup without scheduling
TZ, Set timezone, default="UTC"
""")


def _get_backup_times():
    try:
        backup_time_ref = list(map(int, BACKUP_TIME.split(":")))
        return sorted([f'{(backup_time_ref[0] + i * BACKUP_EVERY_HOUR) % 24:02d}:{backup_time_ref[1]:02d}'
                       for i in range(int(24 / BACKUP_EVERY_HOUR))])
    except (ValueError, IndexError, TypeError):
        logger.error(
            'An error occurred while retrieving the backup times. Please check if BACKUP_TIME has a format like 01:00.')
    except ZeroDivisionError:
        logger.error('An error occurred while retrieving the backup times. BACKUP_EVERY_HOUR can not be 0.')


if TEST_MODE:
    logger.info('Running backup in TEST_MODE.')
    backup()
elif BACKUP_EVERY_HOUR:
    backup_times = _get_backup_times()
    for backup_time in backup_times:
        try:
            logger.info(f'Scheduling hourly backup at {backup_time}')
            schedule.every().day.at(backup_time, TZ).do(backup)
        except Exception as exc:
            logger.exception(f'Exception occurred while scheduling hourly backup at {backup_time}: {exc}')
else:
    try:
        logger.info(f'Scheduling daily backup at {BACKUP_TIME}.')
        schedule.every().day.at(BACKUP_TIME, TZ).do(backup)
    except Exception as exc:
        logger.exception(f'Exception occurred while scheduling daily backup at {BACKUP_TIME}: {exc}')

while True:
    if TEST_MODE:
        break
    try:
        n = schedule.idle_seconds()
        if n is None:
            # no more jobs
            break
        elif n > 0:
            # sleep exactly the right amount of time
            logger.info(
                f'Wait for {time.strftime("%H:%M:%S", time.gmtime(n))} (H:M:S) before next backup job starts running'
            )
            time.sleep(n)
        schedule.run_pending()
    except KeyboardInterrupt:
        logger.info('Terminated by user')
        break
    except Exception as exc:
        logger.exception(f'Exception occurred running scheduler: {exc}')
