import os.path
import logging
from json import load


with open(os.path.abspath('configs/config.json'), 'r') as conf_file:
    configuration = load(conf_file)


setting_configuration = configuration['settings']

try:

    if not os.path.exists(setting_configuration['logs_path']):
        os.mkdir(setting_configuration['logs_path'])

except Exception as exc:

    print('cant create log file in path\n',
          setting_configuration['logs_path'] + '\n',
          exc)

    os.mkdir('logs')

    logging_file = os.path.join(
        'logs',
        setting_configuration['logs_file']
    )

else:

    logging_file = os.path.join(
        setting_configuration['logs_path'],
        setting_configuration['logs_file']
    )

logging.basicConfig(
    level=logging.DEBUG,
    filename=logging_file,
    filemode=setting_configuration['logs_file_mod'],
    format="__%(name)s__: %(asctime)s __%(levelname)s__   \n%(message)s"
)

