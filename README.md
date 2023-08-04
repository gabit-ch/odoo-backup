# odoo-backup

## What is odoo-backup?
odoo-backup is a stateless docker service that creates rotating backups of odoo and uploads them to any sFTP server. The image is based on python alpine and was coded to use minimal resources.

## Main features
- Stateless backup service for odoo databases
- Upload backup to sftp target
- Set scheduling for backups
- Set maximum backups per day, month and year
- Set time based on your timezone

## Quick reference
* Contributed by:<br>GABIT Marco Gantenbein
* Docker Image:<br>[gabitch/odoo-backup](https://hub.docker.com/r/gabitch/odoo-backup)

## Environment Variables
* ```ODOO_URL``` *required - URL of odoo instance. For docker in the same network the container name could be used like "http://odoo:8069"
* ```ODOO_MASTER_PWD``` *required - odoo master password
* ```ODOO_DB_NAME``` *required - name of odoo database to back up
* ```ODOO_BACKUP_FORMAT``` **optional - backup format zip or dump, default=zip
* ```SFTP_HOST```  *required - sFTP server address as domain or IP
* ```SFTP_USER```  *required - sFTP server user
* ```SFTP_PASSWORD```  *required - sFTP server password
* ```SFTP_PORT``` **optional -  port of sFTP Server, default=22
* ```SFTP_PATH``` **optional -  path on sFTP Server, default="/"
* ```TZ``` **optional, set timezone, default="UTC"
* ```BACKUP_TIME``` **optional, start time for backup or time which daily backup should be preserved, default="02:00"
* ```BACKUP_EVERY_HOUR``` **optional, set hours between the backups if hourly backup should be done, default=None
* ```HOURLY_BACKUP_KEEP``` **optional, set count of hourly backups to keep (total hours = BACKUP_EVERY_HOUR * HOURLY_BACKUP_KEEP), default=4
* ```DAILY_BACKUP_KEEP``` **optional, set count of daily backups to keep, default=30
* ```MONTHLY_BACKUP_KEEP``` **optional, set count of monthly backups to keep, default=12
* ```YEARLY_BACKUP_KEEP``` **optional, set count of yearly backups to keep (unlimited=-1), default=-1
* ```TEST_MODE``` **optional, set TEST_MODE=True to directly execute backup without scheduling

## Releases
### 1.0.1
* Add HOURLY_BACKUP_KEEP as a new environment variable
* Add docstrings to script, class and methods
* Fix cleanup process with remaining backups

### 1.0.0
First relaese with all base functions and environment variables

## Known Issues
None

## Roadmap
* Add automated tests
