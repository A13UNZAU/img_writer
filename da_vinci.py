# importing required modules
#Author: Abraham Nzau
import PIL
import pika
import logging
import logging.handlers
import json
import requests
import sys
import time
import os
import pathlib

'''
Use thread-pool libraries from http://github/kim.kiogora
'''

from PIL import Image, ImageFont, ImageDraw
from urllib.request import urlretrieve
from ast import literal_eval

from datetime import datetime


class Davinci:
    username = ''
    password = ''
    queue = ''
    log_path = ''
    logger = None
    rabbit_mq_user = None
    rabbit_mq_pass = None
    server = ''
    port = 5672
    num_threads = 0
    route = ''

    def __init__(self, username, password, queue, exchange, server, port, rabbit_mq_user, rabbit_mq_pass, log_path,
                 num_threads, route):
        self.username = username
        self.password = password
        self.server = server
        self.port = port
        self.rabbit_mq_user = rabbit_mq_user
        self.rabbit_mq_pass = rabbit_mq_pass
        self.queue = queue
        self.exchange = exchange
        self.log_path = log_path
        self.num_threads = num_threads
        self.route = route

        self.logger = logging.getLogger(self.__class__.__name__)
        handlerInfo = logging.handlers.RotatingFileHandler(self.log_path,
                                                           maxBytes=100 * 1024,
                                                           backupCount=5)
        formatter = logging.Formatter('%(asctime)s | %(name)s_%(levelname)s | %(message)s')
        handlerInfo.setFormatter(formatter)
        self.logger.addHandler(handlerInfo)
        self.logger.setLevel(logging.DEBUG)
        self.log_message("The Meastro is starting up ######")

    '''
        Log a message
    '''

    def log_message(self, message):
        self.logger.info('%s' % str(message))

    '''
       Process the image
    '''

    def image_processing(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.log_message("[x] Received %s " % str(body))
        self.log_message("[x] Extracting message >>> ")
        # Extract Message Params
        content = self.get_params(body)

        self.log_message(" Download the image and place it in local storage ###")
        user_msisdn = content['msisdn']
        image_url = content['image_url']
        file_name = user_msisdn + '_' + os.path.basename(image_url)
        # function to return the file extension
        file_extension = pathlib.Path(file_name).suffix

        directory = 'storage/' + user_msisdn
        # Path
        path = os.path.join(os.getcwd(), directory)
        font_path = os.path.join(os.getcwd(), 'fonts')

        isdir = os.path.isdir(path)
        if not isdir:
            self.log_message("[x] Directory does not exist...Proceed to create")
            # Create the directory
            try:
                os.makedirs(path)
                self.log_message("Directory '%s' created successfully" % directory)
            except OSError as error:
                self.log_message("Directory '%s' can not be created" % directory)
        else:
            self.log_message("[x] Directory already exists...")

        self.log_message("[x] image download and save with filename %s" % file_name)
        full_file_name = os.path.join(path, file_name)
        urlretrieve(image_url, full_file_name)
        self.log_message("[x] Retrieved file from url....")

        # process the image
        my_image = Image.open(full_file_name)
        _font = str(font_path) + '/' + str(content['font'])
        self.log_message("[x] Font to be used found as .... %s" % _font)

        '''
        Expected value for font e.g 'playfair/static/PlayfairDisplay-Black.ttf'
        '''
        title_font = ImageFont.truetype(_font, 200)
        title_text = content['text']
        self.log_message("[x] Proceed to write the following text .... %s" % title_text)
        image_editable = ImageDraw.Draw(my_image)

        '''
         Order:: Starting Coordinates, Text, Text color in RGB format, FontStyle
         Pillow library uses a Cartesian pixel coordinate system, with (0,0) in the upper left corner.
         coordinates e.g 15, 15
         color expected in RGB format e.g 237, 230, 211
        '''
        # some minor cleaning to convert hex color from portal to RGB
        hex_color = content['color']

        self.log_message("[x] Found HEX Color as .... %s" % hex_color)
        clean_hex_color = hex_color.lstrip('#')
        rgb_color = tuple(int(clean_hex_color[i:i + 2], 16) for i in (0, 2, 4))

        self.log_message("[x] Extracted RGB color from HEX as .... %s" % str(rgb_color))

        co_ordinates = content['coordinates']
        self.log_message("[x] Extract coordinates as .... %s" % str(co_ordinates))
        image_editable.text(literal_eval(co_ordinates), title_text, rgb_color, font=title_font)

        _str_time = str(datetime.now().strftime("%Y%m%d%H%M%s"))
        self.log_message("[x] Str time ....%s" % _str_time)

        _ext = str(file_extension)
        self.log_message("[x] File Extension => %s" % _ext)

        final_image_name = _str_time + '' + _ext
        self.log_message("[x] Final image Name==>%s" % final_image_name)

        try:
            my_image.save(str(path) + '/' + str(final_image_name))
            self.log_message("[x] Final image saved successfully....")
        except OSError as error:
            self.log_message("[x] Error encountered saving final image %s" % error)

        self.log_message("[x] Make POST request with new image name...")
        ack_request = {'id': content['request_id'], 'final_image_name': final_image_name}
        ack_response = requests.post(url=content['ack_url'], json=ack_request)

        if ack_response.status_code == 200:
            self.log_message(" ack successful %s " % str(ack_response.text))
            return True
        else:
            self.log_message(" image ack request failed %s " % str(ack_response.text))
            return False

    '''
    Get item from JSON packet
    '''

    def get_item(self, dataset, key):
        try:
            json_body = json.loads(dataset)
            value = json_body[key]
        except:
            value = None
        return value

    '''
    Get item from JSON packet
    '''

    def get_inner_item(self, dataset, key):
        try:
            value = dataset[key]
        except:
            value = None
        return value

    def get_params(self, body):
        data = {}
        data['msisdn'] = self.get_item(body, 'msisdn')
        data['image_url'] = self.get_item(body, 'image_url')
        data['font'] = self.get_item(body, 'font')
        data['text'] = self.get_item(body, 'text')
        data['coordinates'] = self.get_item(body, 'coordinates')
        data['color'] = self.get_item(body, 'color')
        data['request_id'] = self.get_item(body, 'request_id')
        data['ack_url'] = self.get_item(body, 'ack_url')
        self.log_message("[x] logging all params %s " % str(data))
        return data

    '''
    Run the application
    '''

    def run(self):
        self.log_message("Processor started up OK ..starting consumer thread ###")

        r_credentials = pika.PlainCredentials(self.rabbit_mq_user, self.rabbit_mq_pass)
        try:
            parameters = pika.ConnectionParameters(self.server, self.port, '/', r_credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange=self.exchange, exchange_type='direct',
                                     passive=False, durable=True, auto_delete=False)
            channel.queue_declare(queue=self.queue, durable=True)
            channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.route)
            self.log_message('[*] Waiting for requests')
            channel.basic_qos(prefetch_count=self.num_threads)
            channel.basic_consume(self.queue, self.image_processing)
            channel.start_consuming()
        except:
            error = str(sys.exc_info()[1])
            self.log_message('[*} Error on consume %s sleep then re-try' % error)
            self.log_message('[*] Reconnecting ...')
            while (True):
                time.sleep(2)
                parameters = pika.ConnectionParameters(self.server, self.port, '/', r_credentials)
                try:
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    channel.queue_declare(queue=self.queue, durable=True)
                    self.log_message('[*] Restored - Waiting for requests ***')
                    channel.basic_qos(prefetch_count=self.num_threads)
                    channel.basic_consume(self.image_processing, queue=self.queue)
                    channel.start_consuming()
                    break
                except:
                    self.log_message('[*] MQ is down - will retry after 2 sec(s)')

'''
    MAIN ROUTINE
'''

username = 'username'
password = 'password'
exchange = "MQ_EXCHANGE"
queue = "QUEUE_NAME"
route = "queue_route"
log_path = "log_path/info.log"
server = '127.0.0.1'
port = 5672
rabbit_mq_user = 'guest'
rabbit_mq_pass = 'guest'

num_threads = 10

# init

pr = Davinci(username, password, queue, exchange, server, port, rabbit_mq_user, rabbit_mq_pass, log_path,
             num_threads, route)
try:
    pr.run()
except:
    error = str(sys.exc_info()[1])
    print("System Shutdown %s" % error)

