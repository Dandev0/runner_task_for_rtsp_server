import subprocess
import shlex
import multiprocessing


class Ffmpeg_rtsp:
    def __init__(self, rtsp_url: str = None, output_url: str = 'localhost', transport='-rtsp_transport tcp'):
        self.__transport = transport
        self.__rtsp_url = rtsp_url
        self.__output_url = output_url

    def __str__(self):
        return f'ffmpeg {self.__transport} -i {self.__rtsp_url} -codec copy -f flv -flvflags no_duration_filesize -f rtsp {self.__output_url}'

    def add_rtsp_camera_to_rtsp_server(self):
        args = shlex.split(self.__str__())
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        p.communicate()[0]

class Create_process_to_ffmpeg(Ffmpeg_rtsp):
    def create_process_to_ffmpeg(self):
        multiprocessing.Process(target=self.add_rtsp_camera_to_rtsp_server).start()


