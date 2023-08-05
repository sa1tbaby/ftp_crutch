from time import sleep as time_sleep
from multiprocessing import Process as mltipr_Process
from multiprocessing import set_start_method as mltipr_set_start_method
from multiprocessing import Queue as mltipr_Queue

def create_process(args,
                   process_name):

    script_process = mltipr_Process(
        target=process_name,
        daemon=True,
        args=args
    )

    return script_process


def script_status(queue,
                  script_process):

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


def get_conf():
    pass


if __name__ == "__main__":

    mltipr_set_start_method('spawn')

    import app
    from app.manager_flask import *
    from app import listener, worker_get, worker_send

    queue_listing_os = mltipr_Queue()
    queue_listing_ftp = mltipr_Queue()
    queue_health = mltipr_Queue()
    queue_status = mltipr_Queue()

    status = False

    flask_process = create_process(args=(queue_listing_ftp, queue_listing_os, queue_status),
                                   process_name=flask_start)
    flask_process.start()


    while True:

        if not script_status(queue_health):

            del proc_listener

            proc_listener = create_process(args=(queue_listing_ftp, queue_listing_os, queue_status),
                                            process_name=)

            proc_listener.start()

        if not ftp_list.empty():
            if not script_status(queue_health):
                del proc_listener

                proc_worker_get = create_process(args=(queue_listing_ftp, queue_listing_os, queue_status),
                                               process_name=)

                proc_worker_get.start()

        if not os_list.empty():
            if not script_status(queue_health):
                del proc_listener

                proc_worker_send = create_process(args=(queue_listing_ftp, queue_listing_os, queue_status),
                                               process_name=)

                proc_worker_send.start()



        time_sleep(app.setting_configuration['script_is_alive_timer'])