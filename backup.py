# Maintainer: GABIT Marco Gantenbein
# Licence: GPLv3

"""This script sets up and manages the scheduling of the backup function depending on the mode the script is running in.

If the script is running in TEST_MODE, it logs that it is running in this mode and directly executes the backup
function.

If the script is set to make hourly or daily backups, it schedules the backup job for the times of the
day specified in the backup_times list. Exceptions during scheduling are logged and do not interrupt the script.

If the script is not set to make backups every hour, it will schedule a daily backup for the time set in BACKUP_TIME.
Exceptions during scheduling are logged and do not interrupt the script.

During scheduled operation (not in TEST_MODE), the script runs in an infinite loop. In each iteration of the loop,
the script sleeps until the next scheduled job, executes any pending scheduled jobs, and handles errors.

Keyboard interrupts are caught and used to exit the loop, logging a relevant message.
Other exceptions are logged and don't interrupt the script.
"""

import logging
import os
import re
import time
import threading
import warnings
import xmlrpc.client
from datetime import datetime
from urllib.parse import urljoin

import pysftp
import requests
import schedule
from dateutil.relativedelta import relativedelta
from paramiko.ssh_exception import SSHException
from pytz import timezone, UnknownTimeZoneError

warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ENV = os.environ
get_env = ENV.get

CNOPTS = pysftp.CnOpts()
CNOPTS.hostkeys = None


def get_env_or_default(key, converter, default=None):
    value = get_env(key)
    if not value:
        return default if default is not None else converter()

    return converter(value)


URL = get_env('ODOO_URL')
MASTER_PWD = get_env('ODOO_MASTER_PWD')
NAME = get_env('ODOO_DB_NAME')
FORMAT = get_env_or_default('ODOO_BACKUP_FORMAT', converter=str, default="zip").lower()
BACKUP_TIME = get_env_or_default('BACKUP_TIME', converter=str, default="02:00")
BACKUP_EVERY_HOUR = get_env_or_default('BACKUP_EVERY_HOUR', converter=int)
HOURLY_BACKUP_KEEP = get_env_or_default('HOURLY_BACKUP_KEEP', converter=int, default=4)
DAILY_BACKUP_KEEP = get_env_or_default('DAILY_BACKUP_KEEP', converter=int, default=30)
MONTHLY_BACKUP_KEEP = get_env_or_default('MONTHLY_BACKUP_KEEP', converter=int, default=12)
YEARLY_BACKUP_KEEP = get_env_or_default('YEARLY_BACKUP_KEEP', converter=int, default=-1)
SFTP_HOST = get_env('SFTP_HOST')
SFTP_PORT = get_env_or_default('SFTP_PORT', converter=int, default=22)
SFTP_USER = get_env('SFTP_USER')
SFTP_PASSWORD = get_env('SFTP_PASSWORD')
SFTP_PATH = get_env_or_default('SFTP_PATH', converter=str, default="/")
TEST_MODE = get_env('TEST_MODE')
TIME_ZONE = get_env_or_default('TZ', converter=str, default='UTC')

try:
    TZ = timezone(TIME_ZONE)
except UnknownTimeZoneError:
    TZ = timezone('UTC')


class SFTPHandler:
    """This class encapsulates actions that can be performed on an SFTP server.

    The SFTPHandler class has methods for uploading and removing files, listing files in a directory, and closing the
    connection.
    Upon initialization, it uses pysftp to create a connection to an SFTP server using provided SFTP hostname, port,
    username, and password.

    Attributes:
        sftp: a pysftp connection object

    Methods:
        - upload(file_path: str, file_name: str)
          Uploads a file to the SFTP server
        - remove(file: str)
          Removes a file from the SFTP server
        - list_files()
          Returns a list of all files in a specific path on the SFTP server
        - close()
          Closes the connection with the SFTP server
    """

    def __init__(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD,
                                      cnopts=cnopts)

    def reconnect(self):
        """Reestablishes the SFTP connection.

        This method first attempts to close any existing SFTP connection. If this
        connection is already closed, it ignores any resulting exceptions. Then, it
        opens a new SFTP connection.

        This is useful in long-running applications, where the SFTP connection might
        become inactive or closed, and need to be reestablished.

        :return: None
        """
        try:
            self.sftp.close()
        except Exception:
            pass
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD,
                                      cnopts=cnopts)

    def upload(self, file_path, file_name, retries=3):
        """Uploads a file to the SFTP server.
        :param file_path: str
            The path to the file on the local system which needs to be uploaded.
        :param file_name: str
            The name that the uploaded file should have on the SFTP server.
        :param retries: int
            The number of connection retries, if connection drops in between.
        :return: None
        """
        for _ in range(retries):
            try:
                self.sftp.put(file_path, os.path.join(SFTP_PATH, file_name))
                break  # if upload was successful, break out of the retry loop
            except SSHException as e:
                logger.error(f"SFTP session error: {str(e)}, Attempting to reconnect...")
                self.reconnect()  # assuming you have defined a reconnect method
        else:
            logger.error(f'Failed to upload after {retries} retries.')

    def remove(self, file, retries=3):
        """Removes a file from the SFTP server.

        :param file: str
            The name of the file on the SFTP server that should be deleted.
        :param retries: int
            The number of connection retries, if connection drops in
        :return: None
        """
        for _ in range(retries):
            try:
                self.sftp.remove(os.path.join(SFTP_PATH, file))
                break  # if upload was successful, break out of the retry loop
            except SSHException as e:
                logger.error(f"SFTP session error: {str(e)}, Attempting to reconnect...")
                self.reconnect()  # assuming you have defined a reconnect method
        else:
            logger.error(f'Failed to upload after {retries} retries.')

    def list_files(self, retries=3):
        """Lists all files in a specific directory on the SFTP server.
        :param retries: int
            The number of connection retries, if connection drops in
        :return: list
            Returns a list of filenames in the SFTP directory.
        """
        for _ in range(retries):
            try:
                return self.sftp.listdir(SFTP_PATH)
            except SSHException as e:
                logger.error(f"SFTP session error: {str(e)}, Attempting to reconnect...")
                self.reconnect()  # assuming you have defined a reconnect method
        else:
            logger.error(f'Failed to upload after {retries} retries.')

    def close(self):
        """Closes the connection with the SFTP server.

        :return: None
        """
        try:
            self.sftp.close()
        except Exception:
            pass


def backup():
    """This method is responsible for creating a backup of an Odoo database, uploading the backup to an SFTP server,
    and managing old backups in the SFTP server.

    It checks if all necessary parameters are set (URL, MASTER_PWD, NAME, FORMAT, SFTP_HOST, SFTP_USER, SFTP_PASSWORD).
    If all parameters are set, it creates a connection to the Odoo server and starts the backup process.

    First, it creates a .zip or .dump backup of the Odoo database to a local file.
    The name of the backup file contains server version, database name, and the timestamp of when the backup started.

    Then, it uploads the backup file to the SFTP server.

    After the upload, it checks the SFTP server for old backups. If any old backups are found, these are removed from
    the SFTP server.

    If an exception occurs during the backup process, the method will log the exception and exit gracefully.

    :raises xmlrpc.client.ProtocolError: If a protocol error occurs while contacting the Odoo server.
    :raises xmlrpc.client.Fault: If a fault error occurs while contacting the Odoo server.
    :raises pysftp.exceptions.ConnectionException: If a connection error occurs while contacting the SFTP server.
    """
    try:
        if (URL and MASTER_PWD and NAME and FORMAT.lower() in ["zip", "dump"] and
                SFTP_HOST and SFTP_USER and SFTP_PASSWORD):
            common = xmlrpc.client.ServerProxy(urljoin(URL, "/xmlrpc/2/common"))
            version = common.version()
            logger.info(f'*** Starting backup process for odoo {version["server_serie"]} '
                        f'with database "{NAME}" on "{URL}" ***')
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
            logger.info(f'*** Finished backup process for odoo {version["server_serie"]} '
                        f'with database "{NAME}" on "{URL}" ***')
        else:
            _backup_help()
    except (xmlrpc.client.ProtocolError, xmlrpc.client.Fault):
        logger.error(
            'Error occurred during connection to odoo.  \
            Please check if odoo is reachable under the provided ODOO_URL.')
    except pysftp.exceptions.ConnectionException:
        logger.exception('Exception occurred during connecting to sFTP Server. \
                         Please check provided sFTP Credentials')
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
            with open(backup_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
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
            hours = HOURLY_BACKUP_KEEP * BACKUP_EVERY_HOUR
            backups_to_remove.extend(
                filter(lambda x: x[0] <= now - relativedelta(hours=hours) and
                                 x[0].hour != backup_time_to_keep, files)
            )
        backups_to_remove.extend(
            filter(lambda x: x[0] <= now - relativedelta(days=DAILY_BACKUP_KEEP) and
                             x[0].day != 1, files)
        )

        backups_to_remove.extend(
            filter(lambda x: x[0] <= now - relativedelta(months=MONTHLY_BACKUP_KEEP) and
                             x[0].month != 1, files)
        )

        if YEARLY_BACKUP_KEEP != -1:
            backups_to_remove.extend(
                filter(lambda x: x[0] <= now - relativedelta(years=YEARLY_BACKUP_KEEP), files)
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


def threaded_backup():
    # Wrap the backup function in a new thread
    thread = threading.Thread(target=backup)
    # Start the new thread
    thread.start()


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
    threaded_backup()
elif BACKUP_EVERY_HOUR:
    backup_times = _get_backup_times()
    for backup_time in backup_times:
        try:
            logger.info(f'Scheduling hourly backup at {backup_time}')
            schedule.every().day.at(backup_time, TZ).do(threaded_backup)
        except Exception as exc:
            logger.exception(f'Exception occurred while scheduling hourly backup at {backup_time}: {exc}')
else:
    try:
        logger.info(f'Scheduling daily backup at {BACKUP_TIME}.')
        schedule.every().day.at(BACKUP_TIME, TZ).do(threaded_backup)
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
