import os
import sys
import logging
import graypy
import pathlib
import paramiko
import shutil
from dotenv import dotenv_values
from datetime import datetime


def sftp_upload(sftp_host: str, sftp_user: str, sftp_spath: str, sftp_dpath: str, sftp_pass: str = None,
                sftp_key: str = None, sftp_key_pass: str = None, sftp_port: int = 22) -> bool:
    rem_filepath = ""
    try:
        transport = paramiko.Transport((sftp_host, sftp_port))
        if sftp_key is not None and len(sftp_key) > 0:
            private_key = paramiko.RSAKey(filename=sftp_key)
            transport.connect(username=sftp_user, pkey=private_key, password=sftp_key_pass)
        else:
            transport.connect(username=sftp_user, password=sftp_pass)
        sftp = paramiko.SFTPClient.from_transport(transport)

        file_without_path = pathlib.Path(sftp_spath).name

        rem_filepath = f"{sftp_dpath}/{file_without_path}"
        sftp.put(sftp_spath, rem_filepath)

        sftp.close()
        transport.close()
        return True
    except ValueError as e:
        print(e.args)
        return False
    except paramiko.ssh_exception.AuthenticationException:
        logger.error("ssh authentication error")
        return False
    except PermissionError as e:
        logger.error(f"ssh permission denied on destination {sftp_host}:{rem_filepath}: {e.args}")
        return False


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


current_dir = os.path.abspath(os.path.dirname(__file__))
settings = dotenv_values(os.path.join(current_dir, ".env"))

# Read settings
log_method = settings.get("log_method", "file").lower()
log_level = settings.get("log_level", "info").lower()
graylog_host = settings.get("graylog_host")
graylog_port = int(settings.get("graylog_port", 12201))
source_dir = settings.get("source_dir", os.path.join(current_dir, "input"))
source_include_ext = settings.get("source_include_ext")
source_exclude_ext = settings.get("source_exclude_ext")
dest_user = settings.get("dest_user")
dest_pwd = settings.get("dest_pwd")
dest_key = settings.get("dest_key")
dest_key_pwd = settings.get("dest_key_pwd")
dest_host = settings.get("dest_host")
dest_port = int(settings.get("dest_port", 22))
dest_path = settings.get("dest_path")
do_backup_int = int(settings.get("do_backup", 1))
backup_path = settings.get("backup_path", os.path.join(current_dir, "backup"))
if do_backup_int == 0:
    do_backup = False
else:
    do_backup = True

# Logging
logger = logging.getLogger(__name__)
log_levels = {'debug': 10, 'info': 20, 'warning': 30, 'error': 40, 'critical': 50}
logger.setLevel(log_levels.get(log_level, 20))

# Catch unhandled exceptions
sys.excepthook = handle_unhandled_exception

if log_method == "file":
    log_file_name = f"sftptransfer_{datetime.now().strftime('%Y_%m_%d')}.log"
    log_path = os.path.join(current_dir, "log", log_file_name)
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        filename=log_path,
                        filemode='a')
elif log_method == "graylog":
    handler = graypy.GELFUDPHandler(graylog_host, graylog_port)
    logger.addHandler(handler)

logger.info(f"sftptransfer gestartet. Source: {source_dir}, Destination: {dest_user}@{dest_host}:{dest_path} on "
            f"Port {dest_port}")

# Error handling
if dest_host is None or len(dest_host) == 0:
    logger.error("No destination host set.")
    exit()
if dest_user is None or len(dest_user) == 0:
    logger.error("No ssh user set.")
    exit()
if dest_path is None or len(dest_path) == 0:
    logger.error("No destination path set.")
    exit()
if not os.path.exists(source_dir):
    logger.error(f"source path {source_dir} does not exist.")
    exit()
if do_backup and not os.path.exists(backup_path):
    logger.error(f"backup path {backup_path} does not exist.")
    exit()

# Log message if backup is disabled
if not do_backup:
    logger.info("Note: Backups are disabled.")

# Build ext information
if source_include_ext is not None and len(source_include_ext) > 0:
    include_ext = source_include_ext.split("|")
    exclude_ext = None
elif source_exclude_ext is not None and len(source_exclude_ext) > 0:
    include_ext = None
    exclude_ext = source_exclude_ext.split("|")
else:
    include_ext = None
    exclude_ext = None

source_path = pathlib.Path(source_dir)
source_files = list(source_path.rglob("*"))
process_files = []

for source_file in source_files:
    if not source_file.is_file():
        continue

    file_ext = source_file.suffix.lower()
    if file_ext is None or len(file_ext) == 0:
        continue

    if include_ext is not None:
        if file_ext not in include_ext:
            continue
    elif exclude_ext is not None:
        if file_ext in exclude_ext:
            continue

    process_files.append(source_file)

logger.info(f"{len(process_files)} files need processing.")
processed_counter = 0

for source_file in process_files:
    logger.debug(f"Processing {source_file}")
    upload_result = sftp_upload(sftp_host=dest_host,
                                sftp_port=dest_port,
                                sftp_user=dest_user,
                                sftp_pass=dest_pwd,
                                sftp_key=dest_key,
                                sftp_key_pass=dest_key_pwd,
                                sftp_dpath=dest_path,
                                sftp_spath=str(source_file))
    if not upload_result:
        logger.error(f"Error occured at file {source_file}")
        continue

    processed_counter += 1

    if do_backup:
        file_backup_path = os.path.join(backup_path, pathlib.Path(source_file).name)
        try:
            shutil.move(source_file, file_backup_path)
        except shutil.Error as err:
            logger.error(f"Error while moving file {source_file} to backup path {file_backup_path}: "
                         f"{err.args}")

logger.info(f"Processed {processed_counter} files.")
