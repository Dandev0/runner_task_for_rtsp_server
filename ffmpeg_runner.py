import subprocess
import shlex


class Ffmpeg_rtsp:
    def __init__(self, rtsp_url: str = None, output_url: str = 'localhost', transport='-rtsp_transport tcp'):
        self.transport = transport
        self.__rtsp_url = rtsp_url
        self.__output_url = output_url

    def __str__(self):
        return f'ffmpeg {self.transport} -i {self.__rtsp_url} -codec copy -f flv -flvflags no_duration_filesize -f rtsp {self.__output_url}'

    def add_rtsp_camera_to_rtsp_server(self):
        args = shlex.split(self.__str__())
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        p.communicate()[0]

