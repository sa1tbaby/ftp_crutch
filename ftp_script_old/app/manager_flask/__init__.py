from flask import Flask

flask_app = Flask(__name__)

from app.manager_flask.flask_views import InfoPage



flask_app.add_url_rule("/", view_func=InfoPage.as_view("info_page"))



