
import cv2
import os, logging, sys, datetime, time
import math, ssl
import cherrypy as HttpServelet
from pymongo import MongoClient
import paho.mqtt.client as mqtt
import json


class VultusBase(object):
    '''
    VultusBase is the database backend for VultusCore
    '''

    mqttc = None
    def __init__(self, dbaddress=None, staticdir=None, msgserver=None):
        """
        Intialize DB and Messaging client
        :param dbaddress:
        :param staticdir:
        :param msgserver:
        """
        ##Initialize Static Dir
        self.staticdir = os.path.join(os.getcwd(), 'ui_www')
        if staticdir:
            self.staticdir = staticdir
        logging.info("Static directory for web-content: %s" % self.staticdir)

        ##Initalize MongoDB
        self.dbaddress = '127.0.0.1'
        self.dbport = 27017
        if dbaddress:
            dbarr = dbaddress.split(':')
            self.dbaddress = dbarr[0]
            if len(dbarr[1]):
                self.dbport = int(dbarr[1])
        logging.info('Connecting to MongoDB {}: {}'.format(self.dbaddress,
                                                           self.dbport))
        client = MongoClient(self.dbaddress, self.dbport)
        self.dbase = client['vultus']
        self.dbcol = self.dbase['agender']

        # Intialize MQTT
        self.mqttserver = 'mqtt.sinhamobility.com'
        if msgserver:
            self.mqttserver = msgserver

        self.initmqtt()

    # MQTT Client Initialization
    def initmqtt(self):
        """
        Initialize MQTT settings
          :return:
       """
       # MQTT Connections
        clientid = "ap{}".format(self.epoch())
        logging.info('Connecting to MQTT Broker({}) with client-id {}'.format(self.mqttserver, clientid))
        self.mqttc = mqtt.Client(clientid, clean_session=True,
                                 transport="tcp",
                                 protocol=mqtt.MQTTv311)

        self.mqttc.username_pw_set("apiuser", "millionchamps")
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.on_subscribe = self.on_subscribe
        # TLS port is 8883, regular TCP is 1883
        self.mqttc.connect("mqtt.sinhamobility.com", 1883, 60)


    # MQTT
    def on_connect(self, client, userdata, flags, rc):
        """
        MQTT callback for on_connect
        :return:
        """
        self.mqttc.subscribe("camerastats", qos=0)
        logging.info('Connection to MQTT Broker established with status {}'.format(rc))

    # MQTT
    def on_message(self, client, userdata, message):
        """
        MQTT on_message callback
          :param client:
          :param userdata:
          :param msg:
          :return:
        """
        print("RXvd: {}/\n\t {}".format(message.topic, message.payload))
        logging.info('MQTT RCVD: {}'.format(message.payload))

    def on_disconnect(self, client, userdata, message):
        print("Disconnected, trying to re-intiallize")
        self.mqttc.disconnect()


    def on_subscribe(self, client, obj, mid, granted_qos):
        """
        :param client:
        :param obj:
        :param mid:
        :param granted_qos:
        :return:
        """
        print("Subscribed")

    def epoch(self):
        """
        Returns Unix Epoch
        """
        epc = int(time.time() * 1000)

        return epc


if __name__ == '__main__':
    print("hello .. ")
    capth = os.getcwd()
    logpath = os.path.join(os.getcwd(), 'log', 'vultuscore.log')
    logdir = os.path.dirname(logpath)
    os.makedirs(logdir, exist_ok=True)

    wwwbase = os.path.join(capth, 'ui_www')
    logging.basicConfig(filename=logpath, level=logging.DEBUG, format='%(asctime)s %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(handler)

    ag = VultusBase(staticdir=wwwbase)
    print("Going to loop")
    ag.mqttc.loop_forever()

