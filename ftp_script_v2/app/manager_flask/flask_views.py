from pandas import DataFrame

from flask.views import View
from flask import render_template
from multiprocessing import Queue


class InfoPage(View):

    def __init__(self,
                 queue_status_local: Queue,
                 queue_health_local: Queue,
                 queue_listing_os_local: Queue,
                 queue_listing_ftp_local: Queue
                 ):

        self.__tmp_data = {'listing_ftp': DataFrame(),
                           'listing_os': DataFrame(),
                           'health': None,
                           'status': None,
                           'ftp_count': None,
                           'os_count': None}

        self.queue_status_local = queue_status_local
        self.queue_health_local = queue_health_local
        self.queue_listing_os_local = queue_listing_os_local
        self.queue_listing_ftp_local = queue_listing_ftp_local

    def dispatch_request(self):

        self.__check_queue('listing_ftp', self.queue_listing_ftp_local)
        self.__check_queue('listing_os', self.queue_listing_os_local)
        self.__check_queue('health', self.queue_health_local)
        self.__check_queue('status', self.queue_status_local)

        if self.__tmp_data is not DataFrame.empty:

            self.__tmp_data['ftp_count'] = len(self.__tmp_data['listing_ftp'])
            self.__tmp_data['os_count'] = len(self.__tmp_data['listing_os'])

        return render_template("info.html",
                               listing_ftp=self.__tmp_data['listing_ftp'],
                               listing_os=self.__tmp_data['listing_os'],
                               health=self.__tmp_data['health'],
                               status=self.__tmp_data['status'],
                               ftp_count=self.__tmp_data['ftp_count'],
                               os_count=self.__tmp_data['os_count'])

    def __check_queue(
            self,
            name,
            queue: Queue
    ):

        if not queue.empty():
            self.__tmp_data[name] = queue.get()