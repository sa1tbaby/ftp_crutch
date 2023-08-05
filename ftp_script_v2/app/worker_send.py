import time
from pandas import DataFrame
from functools import reduce
from static import FTP
from os import remove as os_remove
from os.path import join as os_path_join
import logging


def send_func(doc_list: DataFrame,
              configuration,
              ftp_manager: FTP.ManagerFTP):

    def send_file(path_get,
                  doc_name,
                  file_size) -> bool:

        log_send.debug(f'prepare to send document: {doc_name}')

        try:

            try_count = 0

            while try_count < configuration['worker_get_try_count']:

                # Загрузка документа, True в случае успеха
                # Загрузка .done файла, True в случае успеха
                # Проверка контрольных сумм: file_ftp == file_os -> True
                if ftp_manager.stor(path_get=path_get, file_name=doc_name) and \
                        ftp_manager.stor(path_get=path_get, file_name=(doc_name + '.done')) and \
                        file_size == ftp_manager.get_size(mode='ftp', path_to=None, file_name=doc_name):

                    log_send.debug(f'sending document complete: {doc_name}')
                    return True

                else:
                    try_count += 1
                    log_send.debug(f'something wet wrong with get_file {doc_name}, start try №{try_count}')

            return False

        except Exception as exc:
            log_send.error(f'\n!!!!!!!!!!!!!Critcial error in worker_get!!!!!!!!!!!!! \n'
                          f'the try_count in get_file has more then it possible\n'
                          f'doc_name:{doc_name}', exc_info=exc)

            #save_list(doc_list)
            raise

    def del_file(path_get: str,
                 doc_name: str) -> bool:

        try_count = 0
        exc = ''

        while try_count < configuration['worker_get_try_count']:

            try:

                os_remove(os_path_join(path_get, doc_name))

                log_send.debug(f'worker_send deleting the doc from os: {doc_name} was success!!')

                return True

            except Exception as excep:
                exc = excep
                try_count += 1

        log_send.error(f'!!!!!!!!!!error_in_worker_send!!!!!!!!!!!!!\n'
                      f'something wet wrong with deleting the doc: {doc_name}',
                      exc_info=exc)

        raise

    #
    #
    #
    #

    log_send = logging.getLogger('ftp_script.worker_send')

    log_send.info('***Start worker_send***')

    try:

        time_start = time.time()

        # Смена дирректории фтп, True в случае успеха
        ftp_manager.cwd(path=configuration['ftp_path'],
                        method='get_func')

        try:

            # Итерирование списка документов для загрузки на фтп
            for iteration, doc_name in enumerate(doc_list['file_name']):

                if send_file(path_get=configuration['win_path'],
                             doc_name=doc_name,
                             file_size=doc_list.loc[iteration, 'file_size']):

                    # Установка маркера успешной загрузки
                    doc_list.loc[iteration, 'control_check'] = True

                    del_file(path_get=configuration['win_path'],
                             doc_name=doc_name)
                    del_file(path_get=configuration['win_path'],
                             doc_name=doc_name + '.done')

        except LookupError:

            log_send.critical(f'!!!!!!!!!!error_in_worker_send!!!!!!!!!!!!!\n'
                              f'something wet wrong with iterate list, '
                              f'before sending documents start:', exc_info=True)
            raise

        else:

            success_count = reduce(lambda x, y: x + y, doc_list.control_check)

            log_send.info('sending documents from os has been success! \n'
                          f'total time was: {time.time() - time_start}, \n'
                          f'success doc count was: {success_count} \n'
                          f'ERROR doc count was: {len(doc_list.control_check) - success_count}')

    except MemoryError as exc:
        log_send.critical(f'!!!!!!!!!!error_in_worker_send!!!!!!!!!!!!!\n'
                         f'something with MemoryError wet wrong '
                         f'while getting document has been proceeding:',
                         exc_info=exc)
        raise

    except RuntimeError as exc:
        log_send.error(f'!!!!!!!!!!error_in_worker_send!!!!!!!!!!!!!\n'
                      f'something with RuntimeError wet wrong '
                      f'while getting document has been proceeding:',
                      exc_info=exc)
        raise