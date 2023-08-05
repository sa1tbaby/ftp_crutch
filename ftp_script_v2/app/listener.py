from time import sleep
from static import FTP, Timer
from ftplib import all_errors
import logging


def listen_func(
        queue_listing_os_local,
        queue_listing_ftp_local,
        queue_status_local,
        configuration
):

    def check_put(
            queue,
            file
    ):
        if not queue.empty():

            while not queue.empty():
                tmp = queue.get()
                del tmp

        queue.put(file)

    def listing_from_os(
            queue_listing_os_local
    ):

        list_os = ftp_manager.get_list_from_os(path=configuration[''])

        check_put(queue=queue_listing_os_local,
                  file=list_os)

        if not list_os.empty:
            return True

        else:
            return False

    def listing_from_ftp(
            queue_listing_ftp_local
    ):
        try:

            list_ftp = ftp_manager.get_list_from_ftp(path=configuration[''])

        except all_errors as exc:

            log_listener.critical('Handled error while try to get the listing from ftp', exc_info=exc)
            return 'error'

        check_put(queue=queue_listing_ftp_local,
                  file=list_ftp)

        if not list_ftp.empty:
            return True

        else:
            return False

    log_listener = logging.getLogger('ftp_script.listener')
    log_listener.info('Start listen directory')

    # создание экзмепляра FTP класса
    ftp_manager = FTP.ManagerFTP(path_conf=configuration)
    connection = ftp_manager.get_connection

    # создание экземпляра таймера
    timer = Timer.Timer(name='listener.timer',
                        loger=logging)

    # Объявление endpoint таймера
    timer.create_timer(['list_from_os',
                        'list_from_ftp'])

    try:

        while True:

            if timer.timer(timer_delay=configuration['script_listing_timer_ftp'],
                           timer_name='list_from_ftp'):

                log_listener.info('start listing documents from ftp!\n'
                                  f'path: {configuration[""]}')

                list_ftp_status = ''
                count = 0

                while list_ftp_status is not bool:

                    log_listener.info(f'try to get list from frp №{count}')

                    list_ftp_status = listing_from_ftp(queue_listing_ftp_local=queue_listing_ftp_local)

                    count += 1

                log_listener.info('listing documents from ftp was end successfully!\n')

            # Таймер для листинга с ОС
            if timer.timer(timer_delay=configuration['script_listing_timer_os'],
                           timer_name='list_from_os'):

                log_listener.info('start listing documents from os!\n'
                                  f'path: {configuration[""]}')

                list_os_status = listing_from_os(queue_listing_os_local=queue_listing_os_local)

                log_listener.info('listing documents from os was end successfully!\n')

            check_put(queue=queue_status_local,
                      file='passing')

            # Для автоматического закрытия процесса по срабатыванию таймера
            if timer.timer(timer_name='script_restart',
                           timer_delay=configuration['listener_restart_timer']):
                connection.close()
                break

            sleep(configuration['script_listener_delay'])

    except Exception as exc:
        log_listener.critical('!!!!!!CRITICAL Exception from Listener!!!!!!!!!!',
                              exc_info=exc)
        connection.close()
        raise


