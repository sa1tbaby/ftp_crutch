import time
from pandas import DataFrame
from functools import reduce
from static import FTP
import logging


def get_func(doc_list: DataFrame,
             configuration,
             ftp_manager: FTP.ManagerFTP):

    def get_file(path_post,
                 doc_name,
                 file_size):

        log_get.debug(f'prepare to send document: {doc_name}')

        try:

            try_count = 0

            while try_count < configuration['worker_get_try_count']:

                # Загрузка документа, True в случае успеха
                # Загрузка .done файла, True в случае успеха
                # Проверка контрольных сумм: file_ftp == file_os -> True
                if ftp_manager.retr(path_post=path_post, file_name=doc_name) and \
                        ftp_manager.retr(path_post=path_post, file_name=(doc_name + '.done')) and \
                        file_size == ftp_manager.get_size(mode='os', path_to=path_post, file_name=doc_name):

                    log_get.debug(f'getting document complete: {doc_name}')
                    return True

                else:
                    try_count += 1
                    log_get.debug(f'something wet wrong with get_file {doc_name}, start try №{try_count}')

            return False

        except Exception as exc:
            log_get.error(f'\n!!!!!!!!!!!!!Critcial error in worker_get!!!!!!!!!!!!! \n'
                          f'the try_count in get_file has more then it possible\n'
                          f'doc_name:{doc_name}', exc_info=exc)

            #save_list(doc_list)
            raise

    def del_file(
            doc_name: str
    ) -> True:

        try_count = 0
        exc = ''

        while try_count < configuration['worker_get_try_count']:

            try:

                connection = ftp_manager.get_connection

                result = connection.delete(doc_name)

                log_get.debug(f'{result}\n'
                              f'worker_get deleting the doc: {doc_name} was success!!')

                return True

            except Exception as excep:
                exc = excep
                try_count += 1

        log_get.error(f'!!!!!!!!!!error_in_worker_get!!!!!!!!!!!!!\n'
                         f'something wet wrong with deleting the doc: {doc_name}',
                         exc_info=exc)

        raise

    #
    #
    #
    #

    log_get = logging.getLogger('ftp_script.worker_get')

    log_get.info('***Start worker_get***')

    try:

        time_start = time.time()

        # Смена дирректории фтп, True в случае успеха
        ftp_manager.cwd(path=configuration['ftp_path'],
                        method='get_func')
        try:

            for iteration, doc_name in enumerate(doc_list['file_name']):

                if get_file(path_post=configuration['win_path'],
                            doc_name=doc_name,
                            file_size=doc_list.loc[iteration, 'file_size']):

                    # Установка маркера успешной загрузки
                    doc_list.loc[iteration, 'control_check'] = True
                    del_file(doc_name)
                    del_file(doc_name + '.done')

        except LookupError:

            log_get.critical(f'!!!!!!!!!!error_in_worker_get!!!!!!!!!!!!!\n'
                             f'something wet wrong with iterate list, '
                             f'before getting documents start:', exc_info=True)
            raise

        else:

            success_count = reduce(lambda x, y: x + y, doc_list.control_check)

            log_get.info('getting documents from FTP has been success! \n'
                         f'total time was: {time.time() - time_start}, \n'
                         f'success doc count was: {success_count} \n'
                         f'ERROR doc count was: {len(doc_list.control_check) - success_count}')

    except MemoryError as exc:
        log_get.critical(f'!!!!!!!!!!error_in_worker_get!!!!!!!!!!!!!\n'
                         f'something with MemoryError wet wrong '
                         f'while getting document has been proceeding:',
                         exc_info=exc)
        raise

    except RuntimeError as exc:
        log_get.error(f'!!!!!!!!!!error_in_worker_get!!!!!!!!!!!!!\n'
                      f'something with RuntimeError wet wrong '
                      f'while getting document has been proceeding:',
                      exc_info=exc)
        raise
