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
        print("== {}".format(self.mqttc) )
        self.mqttc.username_pw_set("apiuser", "millionchamps")
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        self.mqttc.on_disconnect = self.on_disconnect
        # TLS port is 8883, regular TCP is 1883
        self.mqttc.connect("mqtt.sinhamobility.com", 1883, 60)

    # MQTT
    def on_connect(self, client, userdata, flags, rc):
        """
        MQTT callback for on_connect
        :return:
        """
        logging.info('Connection to MQTT Broker established with status {}'.format(rc))
        self.mqttc.subscribe("vultus/camerastats")

    # MQTT
    def on_message(self, client, userdata, msg):
        """
        MQTT on_message callback
          :param client:
          :param userdata:
          :param msg:
          :return:
        """
        logging.info('MQTT RCVD: {}'.format(msg.payload))
        dbobj = json.loads(msg.payload)
        upd = self.dbcol.update(dbobj, dbobj, upsert=True)





    def on_disconnect(self, client, userdata, message):
        print("Disconnected, trying to re-intiallize")
        self.mqttc.disconnect()
        self.initmqtt()


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
    ag.mqttc.loop_forever()

