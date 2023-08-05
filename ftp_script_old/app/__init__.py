import os
import logging
import multiprocessing
from json import load

# <-------------------------загрузка-конфига---------------------------->
with open(os.path.abspath('configs/config.json'), 'r') as conf_file:
    configuration = load(conf_file)


setting_configuration = configuration['settings']
# <--------------------------------КОНЕЦ-------------------------------->

# <--------------------------настройки-очереди-------------------------->

queue_listing_os = multiprocessing.Queue()
queue_listing_ftp = multiprocessing.Queue()
queue_health = multiprocessing.Queue()
queue_status = multiprocessing.Queue()
# <--------------------------------КОНЕЦ-------------------------------->

# <---------------------настройки-логгирования-------------------------->
if not os.path.exists(setting_configuration['logs_path']):
    os.mkdir(setting_configuration['logs_path'])

logging_file = os.path.join(
    setting_configuration['logs_path'],
    setting_configuration['logs_file']
)

logging.basicConfig(
    level=logging.DEBUG,
    filename=logging_file,
    filemode='a',
    format="__%(name)s__: %(asctime)s __%(levelname)s__   \n%(message)s"
)
# <--------------------------------КОНЕЦ-------------------------------->
