import ftplib
import json
from ssl import SSLContext
import os.path
from os import walk
import pandas
import time
import logging
import re


class ManagerFTP:

    """
    Класс для работы с FTP

    * def cwd - смена каталога
    * def get_list_from_os - получение листинга документов из ос
    * def get_list_from_ftp - получение листинга документов из FTP
    * def get_size - Получение размера файла
    * def retr - Получение документа с FTP
    * def stor - Отправка документа на FTP
    """

    def __init__(
            self,
            path_conf: str = 'config.json',
            path_log: str = 'logs.txt',
            connection_ssl: bool = False
    ) -> None:

        self.__config = self.__get_config(path_conf)
        self.__logger = self.__create_logger(path_log)
        self.__connection = self.__connect_FTP(connection_ssl)

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'state'):
            cls.state = super(ManagerFTP, cls).__new__(cls)
        return cls.state

    def cwd(
            self,
            path: str,
            method: str = ''
    ) -> None:

        try:

            # Меняем директорию FTP, на необходимый каталог, если мы уже не в нем
            if self.__check_path(path, 'ManagerFTP.cwd'):
                self.__connection.cwd(path)
                self.__logger.info(f'successfully change current FTP dir to {path}')

        except ftplib.error_reply:
            # raised when an unexpected reply is received from the server.
            self.__logger.critical(f"{ManagerFTP.cwd.__name__}:  "
                                   f"method {method}: "
                                   f"something wet wrong with change FTP path: ",
                                   exc_info=True)
            raise

        except ftplib.error_temp:
            # Raised when response codes in the range 400–499
            self.__logger.critical(f"{ManagerFTP.cwd.__name__}:  "
                                   f"method {method}: "
                                   f"something wet wrong with change FTP path: ",
                                   exc_info=True)
            raise

        except ftplib.error_perm:
            # raised when response codes in the range 500–599
            self.__logger.critical(f"{ManagerFTP.cwd.__name__}:  "
                                   f"method {method}: "
                                   f"something wet wrong with change FTP path: ",
                                   exc_info=True)
            raise

        except ftplib.error_proto:
            # exception raised if reply is received from the server begin with a digit in the range 1-5
            self.__logger.critical(f"{ManagerFTP.cwd.__name__}:  "
                                   f"method {method}: "
                                   f"something wet wrong with change FTP path: ",
                                   exc_info=True)
            raise

    def get_list_from_os(
            self,
            path: str
    ) -> pandas.DataFrame:

        start_time = time.time()

        files_list = self.__crate_data_frame()

        # Итерируем файлы в выбранной директории
        index = 0
        for directory, sub_directory, files in walk(os.path.abspath(path)):

            # Отсеиваем все файлы не похожие на xml или done файлы
            # Формируем массив
            for iteration, file_name in enumerate(files):

                # Если файл подходит по маске, убираем .done часть
                # Создавая тем самым дубликат в самом листинге
                if re.findall(r'.xml.done$', file_name):
                    file_name = file_name[:-5]

                elif not re.findall(r'.xml$', file_name):
                    continue

                files_list.loc[index, 'file_name'] = file_name
                files_list.loc[index, 'control_check'] = False
                files_list.loc[index, 'file_size'] = self.get_size(mode='os', file_name=file_name, path_to=path)
                index += 1

        # Сортируем список
        files_list.sort_values(by='file_name', ascending=True, inplace=True)
        sorted_len = len(files_list) - 1

        # Итерируем конечный список
        for iteration, file_name in enumerate(files_list['file_name']):

            # Дропаем последний элемент. С точки зрения логики работы
            # он всегда будет дубликатом
            if iteration == sorted_len:
                files_list.drop([iteration], inplace=True)
                continue

            # Скипаем первый элемент, если второй является его дубликатом
            if file_name == files_list.loc[iteration + 1, 'file_name']:
                continue

            # Удаляем первый элемент, если он не соответствует второму
            else:
                files_list.drop([iteration], inplace=True)

        # Сбрасываем индексы
        files_list.reset_index(drop=True, inplace=True)

        self.__logger.info(f'time spent on {ManagerFTP.get_list_from_os.__name__}: '
                           f'was {int(time.time() - start_time)}sec')

        return files_list

    def get_list_from_ftp(
            self,
            path: str
    ) -> pandas.DataFrame:

        start_time = time.time()

        # Попытка смены текущего каталога FTP
        self.cwd(
            path,
            ManagerFTP.get_list_from_ftp.__name__
        )

        files_list = self.__crate_data_frame()

        # получаем список файлов из директории FTP
        nlst_tmp = self.__connection.nlst()
        index = 0

        for iteration, file_name in enumerate(nlst_tmp):

            # Если файл подходит по маске, убираем .done часть
            # Создавая тем самым дубликат в самом листинге
            if re.findall(r'.xml.done$', file_name):
                file_name = file_name[:-5]

            elif not re.findall(r'.xml$', file_name):
                continue

            files_list.loc[index, 'file_name'] = file_name
            files_list.loc[index, 'control_check'] = False
            files_list.loc[index, 'file_size'] = self.get_size(mode='ftp', file_name=file_name)
            index += 1

        # Сортируем список
        files_list.sort_values(by='file_name', ascending=True, inplace=True)
        sorted_len = len(files_list) - 1

        # Итерируем конечный список
        for iteration, file_name in enumerate(files_list['file_name']):

            # Дропаем последний элемент. С точки зрения логики работы
            # он всегда будет дубликатом
            if iteration == sorted_len:
                files_list.drop([iteration], inplace=True)
                continue

            # Скипаем первый элемент, если второй является его дубликатом
            if file_name == files_list.loc[iteration + 1, 'file_name']:
                continue

            # Удаляем первый элемент, если он не соответсвует второму
            else:
                files_list.drop([iteration], inplace=True)

        # Сбрасываем индексы
        files_list.reset_index(drop=True, inplace=True)

        self.__logger.info(f'time spent on {ManagerFTP.get_list_from_ftp.__name__}: '
                           f'was {int(time.time() - start_time)}sec')

        return files_list

    def get_size(
            self,
            mode: str,
            file_name,
            path_to=None
    ) -> int:
        """
        При вызове метода с параметром mode='ftp' необходимо убедиться,
        что текущая директория FTP адаптера, совпадает с той
        в которой находится файл file_name='file_name'

        :param mode: Параметр среды, FTP или OS
        :param path_to: По умолчанию None, требуется только для mode='os'
        :param file_name:
        :return: size: int, -1 при неудаче
        """

        match mode:

            case 'ftp':

                try:
                    return self.__connection.size(file_name)

                except Exception as exc:
                    self.__logger.error(f'module:{ManagerFTP.get_size.__name__}. '
                                        f'Cant get size for file: {file_name}',
                                        exc_info=exc)

                    return -1

            case 'os':

                try:

                    return os.path.getsize(
                        os.path.join(path_to, file_name)
                    )

                except Exception as exc:
                    self.__logger.error(f'module:{ManagerFTP.get_size.__name__}. '
                                        f'Cant get size for file: {file_name}, '
                                        f'from dir: {path_to}',
                                        exc_info=exc)
                    return -1

    def retr(
            self,
            path_post,
            file_name,
            file_mode='wb'
    ) -> bool:

        try:

            with open(
                    os.path.join(path_post, file_name),
                    file_mode
            ) as file:

                try:

                    self.__connection.retrbinary(
                        f"RETR {file_name}",
                        file.write
                    )

                except ftplib.error_perm:
                    self.__logger.error(f'module:{ManagerFTP.retr.__name__}. '
                                        f'something wet wrong with getting file from FTP',
                                        exc_info=True)

                    return False

                except ftplib.error_reply:
                    self.__logger.error(f'module:{ManagerFTP.retr.__name__}. '
                                        f'something wet wrong with getting file from FTP',
                                        exc_info=True)

                    return False

        except Exception as exc:
            self.__logger.error(f'module:{ManagerFTP.retr.__name__}. '
                                f'something wet wrong with write or create   file:{file_name}, '
                                f'in dir {path_post}',
                                exc_info=exc)
            return False

        else:

            return True

    def stor(
            self,
            path_get,
            file_name,
            file_mode='rb'
    ):

        try:

            with open(
                    os.path.join(path_get, file_name),
                    file_mode
            ) as file:

                try:

                    self.__connection.storbinary(
                        f"STOR {file_name}",
                        file
                    )

                except ftplib.error_perm:
                    self.__logger.error(f'module:{ManagerFTP.stor.__name__}. '
                                        f'something wet wrong with upload file to FTP',
                                        exc_info=True)

                    return False

                except ftplib.error_reply:
                    self.__logger.error(f'module:{ManagerFTP.stor.__name__}. '
                                        f'something wet wrong with upload file to FTP',
                                        exc_info=True)

        except EOFError:
            self.__logger.error(f'module:{ManagerFTP.stor.__name__}. '
                                f'something wet wrong with write or create   file:{file_name}, '
                                f'in dir {path_get}',
                                exc_info=True)

            return False

        else:

            return True

    def __check_path(
            self,
            path,
            func_name
    ):
        """
        Функция проверки совпадения каталогов,
        поскольку в случае попытки смены текущего каталога на такой-же
        выбрасывается исключение 550, ftplib.error_perm

        :param path:
        :param func_name:
        :return:
        """

        if path != self.__connection.pwd():
            return True

        else:
            self.__logger.warning(f'module: {ManagerFTP.__check_path.__name__}. '
                                  f'FTP adapter already in dir {path},'
                                  f'triggered by func: {func_name}')
            del path, func_name
            return False

    def __crate_data_frame(
            self,
            index='file_name'
    ) -> pandas.DataFrame:

        try:

            files_list = pandas.DataFrame(
                columns=[
                    'file_name',
                    'file_size',
                    'control_check'
                ]
            )

            files_list.astype(
                {
                    'file_name': str,
                    'file_size': int,
                    'control_check': bool
                }
            )

            files_list.set_index(index)

        except Exception:
            self.__logger.critical(f'module: {ManagerFTP.__crate_data_frame.__name__}. '
                                   f'something wet wrong with create base DataFrame')

            raise

        else:

            return files_list

    def __connect_FTP(
            self,
            connection_ssl: bool
    ) -> ftplib.FTP | ftplib.FTP_TLS:

        connection_config = self.__config["ftp_connection"]

        if not connection_ssl:

            try:

                connection = ftplib.FTP(
                    host=connection_config['host'],
                    user=connection_config['user_name'],
                    passwd=connection_config['password'],
                    acct=connection_config['acct'],
                    timeout=connection_config['time_out']
                )

                self.__logger.info(f'Successfully connect to FTP'
                                   f'{connection.welcome}\n'
                                   f'{connection.port}\n'
                                   f'{connection.sock}\n'
                                   f'{connection.lastresp}')

                return connection

            except ftplib.all_errors as exc:

                self.__logger.critical(f'module: {ManagerFTP.__connect_FTP.__name__}. '
                                       f'something wet wrong with establish the FTP connection',
                                       exc_info=exc)

                raise

        else:

            try:

                connection = ftplib.FTP_TLS(
                    host=connection_config["URL"],
                    user=connection_config['user_name'],
                    passwd=connection_config['password']
                )

                self.__logger.info(f'Successfully connect to FTP'
                                   f'{connection.welcome}\n'
                                   f'{connection.port}\n'
                                   f'{connection.sock}\n'
                                   f'{connection.lastresp}')

                return connection

            except ftplib.all_errors as exc:

                self.__logger.critical(f'module: {ManagerFTP.__connect_FTP.__name__}. '
                                       f'something wet wrong with establish the FTP connection',
                                       exc_info=exc)

                raise

    def __get_config(
            self,
            path: str | dict
    ) -> dict:
        """
        Получение конфигурации, можно передать непосредственно путь,
        либо уже готовый конфиг type == dict

        :param path:
        :return:
        """
        try:

            if type(path) is not str:
                return path

            else:
                path = os.path.abspath(path)

                with open(path, 'r') as config_file:
                    config = json.load(config_file)

                return config

        except Exception as exc:

            self.__logger.critical(f'module: {ManagerFTP.__get_config.__name__}. '
                                   f'something wet wrong with get config',
                                   exc_info=exc)

            raise

    def __get_cert(
            self
    ) -> SSLContext:
        try:

            cert_object = SSLContext()
            cert_config = self.__config['cert']
            cert_object.load_cert_chain(certfile=os.path.abspath(cert_config['path_cert']),
                                        keyfile=None,
                                        password=cert_config['pass_cert'])

            return cert_object

        except Exception as exc:

            self.__logger.error(f'module: {ManagerFTP.__get_cert.__name__}.'
                                f'something wet wrong with create cert_object from SSLContext',
                                exc_info=exc)

    def __create_logger(
            self,
            tmp_path_log: str
    ):

        try:

            # Настройки для класса фтп
            tmp_conf = self.__config['ftp_connection']

            logging.basicConfig(
                level=int(tmp_conf['log_level']),
                filename=tmp_path_log,
                filemode='a',
                format="__%(name)s__: %(asctime)s __%(levelname)s__   \n%(message)s"
            )

        except Exception as exc:
            print(f'cant create logger for: '
                  f'{ManagerFTP.__create_logger.__name__}'
                  f'{exc}')

        return logging.getLogger(ManagerFTP.__name__)

    @property
    def get_connection(
            self
    ):
        return self.__connection

    @property
    def get_config(
            self
    ):
        return self.__config
