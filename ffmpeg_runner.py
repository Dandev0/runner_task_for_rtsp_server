import subprocess
import shlex
import multiprocessing
from logger_ import LOGGER, extra_name
from config import RABBITMQ_IP, RABBITMQ_LOGIN
import psutil
import os

logger = LOGGER


class Ffmpeg_rtsp:
    def __init__(self, rtsp_url: str = None, output_url: str = 'localhost', transport='-rtsp_transport tcp'):
        self.__transport = transport
        self.__rtsp_url = rtsp_url
        self.output_url = output_url

    def __str__(self):
        return f'ffmpeg {self.__transport} -i {self.__rtsp_url} -codec copy -f rtsp {self.output_url}'

    def add_rtsp_camera_to_rtsp_server(self):
        try:
            command = self.__str__()
            args = shlex.split(command)
            p = subprocess.Popen(args, stdout=subprocess.PIPE)
            p.communicate()[0]
            logger.info(msg=f'Ffmpeg run task!!!',
                        extra={extra_name: f'{command}'})
        except Exception as ex:
            logger.warning(msg=f'Ffmpeg task have a error!!',
                           extra={extra_name: f'Host: {RABBITMQ_IP}  Username: {RABBITMQ_LOGIN}\nError: {ex}'})

    def get_process(self, name):
        return next((p for p in multiprocessing.active_children() if p.name == name), None)


class Create_process_to_ffmpeg(Ffmpeg_rtsp):
    def create_process_to_ffmpeg(self):
        multiprocessing.Process(target=self.add_rtsp_camera_to_rtsp_server, name=self.output_url).start()


class Terminate_task_for_ffmpeg(Ffmpeg_rtsp):
    def Kill_process(self):
        process = self.get_process(name=self.output_url)
        if process is not None:
            pname = psutil.Process(process.pid)
            cpid = pname.get_children(recursive=True)
            for pid in cpid:
                os.kill(pid.pid, 9)
            process.kill()
            return logger.info(msg=f'Процесс был остановлен!',
                               extra={extra_name: f'Processname: {process}'})
        return logger.info(msg=f'Процесс не был найден! Вероятно, он уже был остановлен!',
                           extra={extra_name: f'Processname: {process}'})
