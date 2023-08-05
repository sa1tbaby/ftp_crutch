from app import *
from app.static import FTP, Timer

log_script = logging.getLogger('ftp_script.main_func')
log_get = logging.getLogger('ftp_script.get_func')
log_send = logging.getLogger('ftp_script.send_func')
log_del_g = logging.getLogger('ftp_script.del_after_get')
log_del_s = logging.getLogger('ftp_script.del_after_send')

send_configuration = configuration['send']
get_configuration = configuration['get']
# дирректории выгрузки для ос и фтп
path_from_os = send_configuration['win_path']
path_from_ftp = get_configuration['ftp_path']










