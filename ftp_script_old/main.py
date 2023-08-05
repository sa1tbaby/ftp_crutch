from time import sleep as time_sleep
from threading import Thread as thrdg_Thread
from multiprocessing import Process as mltipr_Process
from multiprocessing import set_start_method as mltipr_set_start_method


def create_process(args):

    # Запуск скрипта, как доп процесс


    script_process = mltipr_Process(
        target=script,
        daemon=True,
        args=args
    )

    return script_process


def script_status(queue):

    global status

    # Сборщик мусора
    while not queue.empty():
        tmp = queue.get()
        del tmp

    status = script_process.is_alive()

    if status:
        queue.put(True)
        return True

    else:
        queue.put(False)
        return False


if __name__ == "__main__":

    mltipr_set_start_method('spawn')

    import app
    from app.srcipt_ftp.ftp_script import script
    from app.manager_flask import *

    status = False

    # Запуск Flask в потоке из основного процесса
    flask_thread = thrdg_Thread(
        target=flask_app.run,
        daemon=True
    )

    flask_thread.start()

    script_process = create_process((app.queue_listing_ftp, app.queue_listing_os, app.queue_status))

    script_process.start()

    while True:

        if not script_status(app.queue_health):

            del script_process

            script_process = create_process((app.queue_listing_ftp, app.queue_listing_os, app.queue_status))

            script_process.start()

        time_sleep(app.setting_configuration['script_is_alive_timer'])