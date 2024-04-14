from adisconfig import adisconfig
from log import Log
from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from json import loads, dumps
from pprint import pprint
from yuki.video import EP, Scene_Static, Scene_Zoom_In 
from pathlib import Path
from os import mkdir

class Renderer:
    name="yuki-video_renderer"

    def __init__(self):
        self._config=adisconfig('/opt/adistools/configs/yuki-video_renderer.yaml')
        
        self._log=Log(
            parent=self,
            rabbitmq_host=self._config.rabbitmq.host,
            rabbitmq_port=self._config.rabbitmq.port,
            rabbitmq_user=self._config.rabbitmq.user,
            rabbitmq_passwd=self._config.rabbitmq.password,
            debug=self._config.log.debug,
            )

        self._rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                heartbeat=0,
                host=self._config.rabbitmq.host,
                port=self._config.rabbitmq.port,
                credentials=PlainCredentials(
                    self._config.rabbitmq.user,
                    self._config.rabbitmq.password
                )
            )
        )
        self._rabbitmq_channel = self._rabbitmq_conn.channel()

        self._rabbitmq_channel.basic_consume(
            queue='yuki-render_requests',
            auto_ack=True,
            on_message_callback=self._render_request
        )

        self._media_dir=Path(self._config.directories.media)
        self._renders_dir=Path(self._config.directories.renders)


    def _render_request(self, channel, method, properties, body):
        data=loads(body.decode('utf8'))
        pprint(data)
        
        ep=EP(**data['episode_meta'])
        for scene in data['scenes']:
            scene['image']=self._media_dir.joinpath(scene['image'])
            scene['audio']=self._media_dir.joinpath(scene['audio'])
            scene['audio_volume']=1
            if scene['type']=='static':
                ep.add_scene(Scene_Static, **scene)
            elif scene['type']=='zoom_in':
                ep.add_scene(Scene_Zoom_In, **scene)

        ep.render()


    def start(self):
        self._rabbitmq_channel.start_consuming()

    def stop(self):
        self._rabbitmq_channel.stop_consuming()

if __name__=="__main__":
    video_renderer=Renderer()
    video_renderer.start()
