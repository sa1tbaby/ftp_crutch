import pandas
import time
from datetime import datetime
from functools import reduce
from app.srcipt_ftp import *


def script(
        queue_listing_ftp_local,
        queue_listing_os_local,
        queue_status_local
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

    def save_list(
            doc_list: pandas.DataFrame,
            sub_dir=None
    ):

        # Получаем текущую дату и время, форматируем
        # path -> logs/<date.now>
        # file -> /<time.now>
        path_log = str(datetime.now())
        path_log = path_log.replace(':', "-")
        path_log = path_log[:path_log.rfind('.')]
        name = path_log[path_log.find(' ') + 1:]
        path_log = path_log[:path_log.find(' ')]

        path_log = os.path.join(setting_configuration['logs_path'], path_log)

        if not os.path.exists(path_log):
            os.mkdir(path_log)

        if sub_dir is not None:
            path_log = os.path.join(path_log, sub_dir)

            if not os.path.exists(path_log):
                os.mkdir(path_log)

        name = os.path.join(path_log, f'{name}.xlsx')

        doc_list.to_excel(name, index=False)

    def get_func(
            doc_list: pandas.DataFrame
    ) -> True:

        """
        Функция для выгрузки документов с ФТП

        * Cмена каталога фтп на config['get']['ftp_path']
        * Итерирование списка документов для загрузки
            * Получение документа с ФТП
            * Полученеие .done файла с ФТП
            * Сравнеие размеров полученного файла с ОС и с ФТП
            * Установка в списке маркера успешной загрузки 'control_check'
        * Итерирование списка документов для удаления
            * Проверка прохождения контрольных сумм
            * Удаление документа с ФТП
            * Удаление .done файла
            * удлаение информации о документе из списка

        :param doc_list: Список документов для выгрузки с ФТП
        :return: True в случае успеха
        """

        def del_file(
                file_name: str
        ) -> True:

            try:

                result = connection.delete(file_name)
                log_del_g.debug(f'{result}, file: {file_name}')

                result = connection.delete(file_name + '.done')
                log_del_g.debug(f'{result}, file: {file_name}.done')
                return True

            except Exception:
                log_del_g.error('something wet wrong with __send_file, unhandled', exc_info=True)
                raise

        def get_file(
                path_post,
                file_name,
                file_size
        ) -> True:

            log_get.debug(f'prepare to get document: {file_name}')

            try:

                # Счетчик попыток получения файла
                try_count = 0

                while try_count < 3:

                    # Загрузка документа, True в случае успеха
                    # Загрузка .done файла, True в случае успеха
                    # Проверка контрольных сумм: file_ftp == file_os -> True
                    if ftp_manager.retr(path_post=path_post, file_name=file_name) and \
                        ftp_manager.retr(path_post=path_post, file_name=(file_name + '.done')) and \
                            file_size == ftp_manager.get_size(mode='os', path_to=path_post, file_name=file_name):

                        log_get.debug(f'getting document complete: {file_name}')
                        return True

                    else:
                        try_count += 1
                        log_get.debug(f'something wet wrong with get_file {file_name}, start try №{try_count}')

            except Exception:
                log_get.error(f'something wet wrong with getting document: {file_name}', exc_info=True)
                raise

            return False

        try:

            time_start = time.perf_counter()

            log_get.info('starting iterate list from FTP')

            try:

                # Смена дирректории фтп, True в случае успеха
                ftp_manager.cwd(path=get_configuration['ftp_path'],
                                method='get_func')

                # Итерирование списка документов для загрузки c фтп
                for iteration, file_name in enumerate(doc_list['file_name']):

                    file_size = doc_list.loc[iteration, 'file_size']

                    # Загрузка файла, принимает путь к файлу внутри ос, имя и размер файла из списка
                    if get_file(path_post=get_configuration['win_path'],
                                file_name=file_name,
                                file_size=file_size):

                        # Установка маркера успешной загрузки
                        doc_list.loc[iteration, 'control_check'] = True

            except LookupError:
                log_get.error(f'something wet wrong with iterate list, before getting documents start:', exc_info=True)

            #получение количества успешных доков
            success_count = reduce(lambda x, y: x + y, doc_list.control_check)

            log_get.info('getting documents from FTP has been success! \n'
                         f'total time was: {time.perf_counter() - time_start}, \n'
                         f'success doc count was: {success_count} \n'
                         f'ERROR doc count was: {len(doc_list.control_check) - success_count}')

        except MemoryError:
            log_get.error(f'something wet wrong with getting document:', exc_info=True)

        except RuntimeError:
            log_get.error(f'something wet wrong with getting document:', exc_info=True)

        else:

            # Смена дирректории фтп, True в случае успеха
            ftp_manager.cwd(get_configuration['ftp_path'], 'get_func.def_file')

            log_get.info(f'starting delete documents after getting from FTP')

            try:

                # Итерирование списка документов для удаления c фтп
                for iteration, file_name in enumerate(doc_list['file_name']):

                    # Проверка прохождения контрольных сумм
                    if doc_list.loc[iteration, 'control_check']:
                        # Удаление файла с ФТП
                        del_file(file_name)
                        # Удаление файла из списка
                        doc_list.drop(iteration, inplace=True)

            except Exception:
                log_get.error(f'something wet wrong with deleting documents, after they getting from FTP:', exc_info=True)
                raise

            log_get.info('deleting documents from FTP has been success')

            return True

    def send_func(
            doc_list: pandas.DataFrame
    ) -> True:

        """
        Функция для загрузки документов на ФТП

        * Cмена каталога фтп на config['send']['ftp_path']
        * Итерирование списка документов для загрузки
            * Загрузка документа на ФТП
            * Загрузка .done файла на ФТП
            * Сравнеие размеров загруженного файла с ФТП и с ОС
        * Итерирование списка документов для удаления
            * Проверка прохождения контрольных сумм
            * Удаление документа с ОС
            * Удаление .done файла
            * удлаение информации о документе из списка

        :param doc_list: Список документов для загрузки на ФТП
        :return: True в случае успеха
        """

        def del_file(
                path: str,
                file_name: str
        ) -> True:

            try:

                # Удаление документа с ОС
                os.remove(os.path.join(path, file_name))
                log_del_s.debug(f'delete {file_name}: success')

                # Удаление .done файла
                os.remove(os.path.join(path, file_name + '.done'))
                log_del_s.debug(f'delete {file_name}.done: success')

                return True

            except Exception as exc:
                log_del_g.error(f'something wet wrong with deleting {file_name} ,unhandled: \n{exc}')
                raise

        def send_file(
                path_get,
                file_name,
                file_size
        ) -> bool:

            log_send.debug(f'prepare to send document: {file_name}')

            try:

                # Счетчик попыток получения файла
                try_count = 0

                while try_count < 3:

                    # Отправка документа, True в случае успеха
                    # Отправка .done файла, True в случае успеха
                    # Проверка контрольных сумм: file_os == file_ftp -> True
                    if ftp_manager.stor(path_get=path_get, file_name=file_name) and \
                            ftp_manager.stor(path_get=path_get, file_name=(file_name + '.done')) and \
                            file_size == ftp_manager.get_size(mode='ftp', path_to=None, file_name=file_name):

                        log_send.debug(f'sending document complete: {file_name}')

                        return True

                    else:
                        try_count += 1
                        log_get.debug(f'something wet wrong with send_file {file_name}, start try №{try_count}')

            except Exception:
                log_send.error(f'something wet wrong with sending document: {file_name}', exc_info=True)
                raise

            return False

        try:
            time_start = time.perf_counter()

            # Смена дирректории фтп, True в случае успеха
            ftp_manager.cwd(path=send_configuration['ftp_path'],
                            method='send_func')

            log_send.info('starting iterate list from os')

            try:

                # Итерирование списка документов для загрузки на фтп
                for iteration, file_name in enumerate(doc_list['file_name']):

                    file_size = doc_list.loc[iteration, 'file_size']

                    # Отправка файла, принимает путь для создания файла внутри ос, имя и размер файла из списка
                    if send_file(path_get=send_configuration['win_path'],
                                 file_name=file_name,
                                 file_size=file_size):

                        # Установка маркера успешной загрузки
                        doc_list.loc[iteration, 'control_check'] = True

                errors_list = pandas.DataFrame(filter(lambda x: not x[2], doc_list.values))

                if len(errors_list) != 0:
                    save_list(doc_list=errors_list,
                              sub_dir='error')

                del errors_list

            except LookupError:
                log_send.error(f'something wet wrong with iterate list, before sending documents start:', exc_info=True)
                raise

            # получение количества успешных доков
            success_count = reduce(lambda x, y: x + y, doc_list.control_check)

            log_send.info('sending documents from OS has been END!\n'
                          f'total time was: {time.perf_counter() - time_start},\n'
                          f'total doc count was: {len(doc_list)}\n'
                          f'success doc count was: {success_count} \n'
                          f'ERROR doc count was: {len(doc_list.control_check) - success_count}\n')

        except MemoryError:
            log_send.error(f'something wet wrong with sending document:', exc_info=True)
            raise

        except RuntimeError:
            log_send.error(f'something wet wrong with sending document:', exc_info=True)
            raise

        else:

            log_send.info(f'starting delete documents from os, after sending')

            try:

                # Итерирование списка документов для удаления c ОС
                for iteration, file_name in enumerate(doc_list['file_name']):

                    # Проверка прохождения контрольных сумм
                    if doc_list.loc[iteration, 'control_check']:
                        # Удаление файла с ОС
                        del_file(path=send_configuration['win_path'], file_name=file_name)

                        # Удаление файла из списка
                        doc_list.drop(iteration, inplace=True)


            except Exception as exc:
                log_send.error(f'something wet wrong with deleting documents, '
                               f'after they sending on FTP:',
                               exc_info=exc)
                raise

            log_send.info('deleting documents from OS has been success!')

            return True

    log_script.info('Initiate ftp script')

    # создание экзмепляра FTP класса
    ftp_manager = FTP.ManagerFTP(path_conf=configuration)
    connection = ftp_manager.get_connection
    log_script.info('Initiate FTP.ManagerFTP has been success!')

    # создание экземпляра таймера
    timer = Timer.Timer(name='ftp_script.timer',
                        loger=logging)
    log_script.info('Initiate Timer.Timer has been success!')

    # Объявление endpoint таймера
    timer.create_timer(['list_from_os',
                        'list_from_ftp',
                        'send_func',
                        'get_func',
                        'script_restart'])

    while True:

        log_script.info('Starting script iterate!\n')

        try:

            check_put(queue=queue_status_local,
                      file='working')

            # Таймер для листинга ФТП
            if timer.timer(timer_delay=setting_configuration['script_listing_timer'],
                           timer_name='list_from_ftp'):

                log_script.debug('start listing documents from FTP!')
                list_from_ftp = ftp_manager.get_list_from_ftp(path=path_from_ftp)

                # Обновление данных в очереди
                check_put(queue=queue_listing_ftp_local,
                          file=list_from_ftp)

                # Сохранение резервной копии листинга
                save_list(doc_list=list_from_ftp,
                          sub_dir='from_ftp')
                log_script.debug('listing from FTP was saved!')

                # Таймер цикла выгрузки документов с ФТП
                if timer.timer(timer_delay=setting_configuration['script_dwnld_timer'],
                               timer_name='get_func'):

                    log_script.info('start get_func!')

                    # Запуск отправки документов из переданного листинга
                    if get_func(doc_list=list_from_ftp):
                        log_script.info('get_func success!')

            # Таймер для листинга с ОС
            if timer.timer(timer_delay=setting_configuration['script_listing_timer'],
                           timer_name='list_from_os'):

                log_script.debug('start listing documents from OS!')
                list_from_os = ftp_manager.get_list_from_os(path=path_from_os)

                # Обновление данных в очереди
                check_put(queue=queue_listing_os_local,
                          file=list_from_os)

                # Сохранение резервной копии листинга
                save_list(doc_list=list_from_os,
                          sub_dir='from_os')
                log_script.debug('listing from OS was saved!')

                # Таймер цикла загрузки документов на ФТП
                if timer.timer(timer_delay=setting_configuration['script_dwnld_timer'],
                               timer_name='send_func'):

                    log_script.info('start send_func!')

                    if send_func(doc_list=list_from_os):
                        log_script.info('send_func success!')

        except Exception as exc:
            log_script.critical('Exception from ftp script',
                                exc_info=exc)
            connection.close()
            raise

        check_put(queue=queue_status_local,
                  file='passing')

        # Для автоматического закрытия процесса по срабатыванию таймера
        if timer.timer(timer_name='script_restart',
                       timer_delay=setting_configuration['script_restart_timer']):
            connection.close()
            break

        time.sleep(setting_configuration['script_pooling_timer'])


