#!/usr/bin/env python
# -*- coding: utf-8 -*-
from re import T
import pika                
import configparser
import logging
import os
import json
import requests
import threading
import signal
import sys
from time import sleep
import configparser
from linebot import LineBotApi
from linebot.models import TextSendMessage, ImageSendMessage
from linebot.exceptions import LineBotApiError

config = configparser.ConfigParser()
config.read('config.ini')


class MessageSender(object):

    LINE_ACCESS_TOKEN = config.get('LINE', 'ACCESS_TOKEN')
    LINE_USER_ID = config.get('LINE', 'USER_ID')
    TELEGRAM_ACCESS_TOKEN = config.get('TELEGRAM', 'ACCESS_TOKEN')
    TELEGRAM_CHAT_ID = config.get('TELEGRAM', 'CHAT_ID')
    RABBITMQ_HOST = config.get('RABBITMQ', 'HOST')
    RABBITMQ_MESSAGE_QUEUE_NAME = config.get('RABBITMQ', 'MESSAGE_QUEUE_NAME')
    
    def __init__(self, configPath='./MessageSender.cfg'):
        logging.info("[MessageSender] MessageSender initialing")
        if os.path.exists(configPath):
            self.config = configparser.ConfigParser()
            self.config.read(configPath, encoding="UTF-8")
        else:
            logging.info("[MessageSender] %r not found, program will exit" % configPath)
            exit()
        rmqHost = self.RABBITMQ_HOST
        logging.info("[MessageSender] rmqHost: %r" % rmqHost)
        self.rmqMessageQueueName = self.RABBITMQ_MESSAGE_QUEUE_NAME
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rmqHost))
        self.channel = self.connection.channel()

        # Declare the queue
        self.channel.queue_declare(queue=self.rmqMessageQueueName)
        # Consume messages
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.rmqMessageQueueName, on_message_callback=self.consumeToMessage)
        pass
   
    # to message
    def consumeToMessage(self,ch, method, properties, body):
        # Process message
        message = str(body)
        logging.info("[MessageSender] Received message:  %r" % message)
        #to line
        self.sendMessageToMq(message)
        #to telegram
        self.sendMessageToMq(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        pass

    # to telegram
    def consumeToTelegram(self,ch, method, properties, body):
        try:
            logging.info("[MessageSender] consumeToTelegram")
            # Process message
            #message = json.loads(body)
            message = str(body)
            #message = "local message"
            logging.info("[MessageSender] Received message:  %r" % message)
            url = f"https://api.telegram.org/bot{self.TELEGRAM_ACCESS_TOKEN}/sendMessage?chat_id={self.TELEGRAM_CHAT_ID}&text={message}"
            logging.info("[MessageSender] url: %r" % url)
            logging.info("[MessageSender] result from telegram: %r" % requests.get(url).json())
        except Exception as e:
            # Requeue message
            logging.error("[MessageSender] Exception message:  %r" % e)
        pass
    # to line
    def consumeToLine(self,ch, method, properties, body):
        try:
            logging.info("[MessageSender] consumeToLine")
            message = str(body)
            line_bot_api = LineBotApi(self.LINE_ACCESS_TOKEN)
            line_bot_api.push_message(self.LINE_USER_ID, TextSendMessage(text=message))
            logging.info("[MessageSender] Sent message to line:  %r" % message)
            pass
        except Exception as e:
            logging.error("[MessageSender] Exception message:  %r" % e)
        pass

    def StartConsuming(self):
        logging.info("[MessageSender] StartConsuming")
        #self.consumingThread = threading.Thread(target = self.channel.start_consuming)
        #self.consumingThread.start()
        self.channel.start_consuming()
        pass

    def Stop(self):
        logging.info("[MessageSender] Stop")
        try:
            self.channel.stop_consuming()
        except Exception as e:
            logging.error("[MessageSender] Exception message:  %r" % e)

        try:
            self.channel.close()
        except Exception as e:
            logging.error("[MessageSender] Exception message:  %r" % e)
        
        try:
            self.connection.close()
        except Exception as e:
            logging.error("[MessageSender] Exception message:  %r" % e)
        pass

    
    def sendMessageToMq(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.rmqTelegramQueueName, body=str(message),
                      properties=pika.BasicProperties(
                          delivery_mode = 2, # make message persistent
                      ))
            return True
        except Exception as e:
            logging.error("[MessageSender] Exception message:  %r" % e)
            return False
        pass


#def sigterm_handler(_signo, _stack_frame):
#    # Raises SystemExit(0):
#    sys.exit(0)

#if sys.argv[1] == "handle_signal":
#    signal.signal(signal.SIGTERM, sigterm_handler)

if __name__ == '__main__':
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y/%m/%d/ %H:%M:%S %p"
    logging.basicConfig(filename='MessageSender.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
    logging.getLogger().addHandler(logging.StreamHandler())
    messageSender = None
    try:
        messageSender = MessageSender()
        messageSender.sendMessageToMq("[MessageSender] MessageSender is starting...")
        messageSender.StartConsuming()
        #sleep(30)
    except Exception as e:
        logging.error("[MessageSender] Exception message:  %r" % e)
    finally:
        if messageSender is not None:
            messageSender.Stop()
        pass
    pass
