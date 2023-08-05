import pandas

from app import *
from flask.views import View
from flask import render_template


class InfoPage(View):
    tmp_data = {'listing_ftp': pandas.DataFrame(),
                'listing_os': pandas.DataFrame(),
                'health': None,
                'status': None,
                'ftp_count': None,
                'os_count': None}

    def dispatch_request(self):

        self.__check_queue('listing_ftp', queue_listing_ftp)
        self.__check_queue('listing_os', queue_listing_os)
        self.__check_queue('health', queue_health)
        self.__check_queue('status', queue_status)

        try:

            InfoPage.tmp_data['ftp_count'] = len(InfoPage.tmp_data['listing_ftp'])
            InfoPage.tmp_data['os_count'] = len(InfoPage.tmp_data['listing_os'])

        except Exception:
            pass

        return render_template("info.html",
                               listing_ftp=InfoPage.tmp_data['listing_ftp'],
                               listing_os=InfoPage.tmp_data['listing_os'],
                               health=InfoPage.tmp_data['health'],
                               status=InfoPage.tmp_data['status'],
                               ftp_count=InfoPage.tmp_data['ftp_count'],
                               os_count=InfoPage.tmp_data['os_count'])

    def __check_queue(
            self,
            name,
            queue: multiprocessing.Queue
    ):

        if not queue.empty():
            InfoPage.tmp_data[name] = queue.get()