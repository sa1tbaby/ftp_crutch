from flask import Flask

flask_app = Flask(__name__)

from flask_views import InfoPage


def flask_start(queue_status_local,
                queue_health_local,
                queue_listing_os_local,
                queue_listing_ftp_local):

    flask_app.add_url_rule("/", view_func=InfoPage.as_view("info_page",
                                                           queue_status_local,
                                                           queue_health_local,
                                                           queue_listing_os_local,
                                                           queue_listing_ftp_local))

    flask_app.run()