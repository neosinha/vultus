import cv2
import os, logging, sys, datetime, time
import math
from pyagender import PyAgender
import paho.mqtt.client as mqtt
import requests, json
from threading import Thread


class VultusCore(Thread):
    '''
    VultusCore would build a classifier object which can called on multiple images
    '''


    def __init__(self, cascadePath=None, odir=None, msgserver=None, msgtopic=None):
        '''
        Constructor for PixelCore
        '''
        self.cascPath = os.path.join(os.getcwd(),
                                     'models',
                                     'haarcascades',
                                     'haarcascade_frontalface_extended.xml')

        resnetwts = os.path.join(cascadePath, 'models', 'weights.18-4.06.hdf5')
        if cascadePath:
            self.cascPath = os.path.join(os.path.abspath(cascadePath),
                                         'models',
                                         'haarcascades',
                                         'haarcascade_frontalface_alt.xml')

        print('Loading HAAR-Cascade file: %s' % (self.cascPath))
        self.faceCascade = cv2.CascadeClassifier(self.cascPath)
        self.face_size = 32
        self.agender = PyAgender()

        # MQTT Connections
        # Intialize MQTT
        self.mqttserver = 'mqtt.sinhamobility.com'
        self.msgtopic = 'camerastats'
        if msgtopic:
            self.msgtopic = str(msgtopic).strip()

        if msgserver:
            self.mqttserver = msgserver
        clientid = "ap{}".format(self.epoch())
        logging.info('Connecting to MQTT Broker({}) with client-id {}'.format(self.mqttserver, clientid))
        logging.info('Subscribing to topic {}'.format(self.msgtopic))

        self.mqttc = mqtt.Client(clientid, clean_session=True,
                                 transport="tcp",
                                 protocol=mqtt.MQTTv311)

        print("== {}".format(self.mqttc))
        self.mqttc.username_pw_set("apiuser", "millionchamps")

        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        self.mqttc.on_disconnect = self.on_disconnect
        # TLS port is 8883, regular TCP is 1883
        self.mqttc.connect("mqtt.sinhamobility.com", 1883, 60)
        # MQTT Client Initialization
        self.initmqtt()

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
        print("== {}".format(self.mqttc))
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

    # MQTT
    def on_message(self, client, userdata, msg):
        """
        MQTT on_message callback
          :param client:
          :param userdata:
          :param msg:
          :return:
        """
        logging.info('MQTT RCVD: {}'.format(msg))

    def on_disconnect(self, client, userdata, message):
        print("Disconnected, trying to re-intiallize")
        self.mqttc.disconnect()
        self.initmqtt()


    def getLocation(self):
        """
        Uses GeoIP.Net to get GeoLocation
        :return:
        """
        send_url = 'https://ipinfo.io'
        r = requests.get(send_url)
        resp = json.loads(r.text)
        logging.info("GeoLoc: {}".format(resp))
        return resp

    def readGrayScaleImage(self, imgpath=None, frame=None):
        """
        Reads Image File, checka and converts to grayscale if
        needed
        :peram frame: actual frame
        :param imgpath: path to the image
        :return:
        """
        limgpath = None
        if imgpath:
            limgpath = imgpath.strip()
        else:
            return
        print("Loading image: {}".format(limgpath))
        frame = cv2.imread(limgpath)
        # check if grayscale conversion is needed
        grayScale = False
        grayFrame = frame
        print("==>{}".format(len(frame)))
        if len(frame) < 2:
            # strating image is grayscale
            grayScale = True
            logging.info("Image %s is grayscale" % (limgpath))
        else:
            # strating image is color, grayscale conversion is needed
            logging.info("Converting %s to grayscale" % (limgpath))
            grayFrame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        return {'frame': frame, 'gray': grayFrame, 'grayscale': grayScale}
        # return grayFrame

    def facedetectFrame(self, grayframe=None, colorframe=None):
        """
        Face detect within a gray frame
        :return:
        """
        faces = self.faceCascade.detectMultiScale(
            grayframe,
            scaleFactor=1.2,
            minNeighbors=10,
            minSize=(self.face_size, self.face_size)
        )
        oframe = None
        if not colorframe is None:
            oframe = colorframe.copy()

        fcount = 0
        agenderx = self.agender.detect_genders_ages(colorframe)
        # print('Agender: %s' % (agenderx))

        for face in agenderx:
            age = int(math.ceil(face['age'] / 1.0))
            gender = 'M'
            if face['gender'] > 0.5:
                gender = 'F'

            oframe = cv2.rectangle(oframe, (face['left'] - 5, face['top'] - 5),
                                   (face['left'] + face['width'] - 5, face['top'] + face['height'] - 5),
                                   (0, 255, 0),
                                   thickness=1)
            cv2.putText(oframe, '%s %s' % (age, gender),
                        (face['left'], face['top'] + face['height']),
                        cv2.FONT_HERSHEY_SIMPLEX, 2, (0, 255, 0), 1)

        return {'bboxes': oframe, 'faces': faces}

    def processImageFrame(self, frame):
        """
        Process Image Frame
        :return:
        """
        faces = self.agender.detect_genders_ages(frame)
        ts2 = self.epoch()
        pfnamex = "{}-agender.JPG".format(ts2)
        wwwbase = os.getcwd()
        odir = os.path.join(wwwbase, 'agender')
        if not os.path.exists(odir):
            logging.info('Creating output directory, %s' % (odir))
            os.makedirs(odir)
        pfname = os.path.join(wwwbase, 'agender', pfnamex)
        logging.info('Writing image file, %s' % (pfname))
        cv2.imwrite(pfname, frame)
        return

    def process_frame(self, imgfile=None, statticdir=None):
        """
        :return:
        """
        images = self.readGrayScaleImage(imgpath=imgfile)
        faces = self.facedetectFrame(grayframe=images['gray'], colorframe=images['frame'])
        for face in faces:
            print(face)

        odirx = os.path.join(statticdir, 'agender')
        if not os.path.exists(odirx):
            os.makedirs(odirx)

        ofilex = os.path.join(odirx, '{}.jpg'.format(self.epoch()))
        cv2.imwrite(ofilex, faces['bboxes'])


    def analyze_video_file(self, vidfile=None, droprate=50):
        """
        Would analyze a video file and then generate the output in a file
        :param vidfile:
        :return:
        """
        vreader = None
        vdevice = None
        vreader = cv2.VideoCapture(vidfile)
        framecount = 0
        vidstats = []
        print("Analyzing video file: %s" % (vidfile))
        fps = int(math.ceil(vreader.get(cv2.CAP_PROP_FPS)))
        maxframes = vreader.get(cv2.CAP_PROP_FRAME_COUNT)
        width = vreader.get(cv2.CAP_PROP_FRAME_WIDTH)  # float
        height = vreader.get(cv2.CAP_PROP_FRAME_HEIGHT)
        dur = int(maxframes / fps)
        dt = "%s" % (datetime.datetime.now())
        dt = dt.split('.')[0]
        print("Starting processing video {} File: {} Duration: {},"
              "Ht: {}, Wd: {} MaxFrames: {}, FPS: {}".format(dt, vidfile, dur, width, height, maxframes, fps))

        ovideo = vidfile.replace('.mp4', '-out.mp4')
        fourcc = cv2.VideoWriter_fourcc(*"H264")
        vwriter = cv2.VideoWriter(ovideo, fourcc, int(fps), (int(width), int(height)))
        fnum = 0
        # prev frame
        pframe = None
        fgbg = cv2.createBackgroundSubtractorMOG2()
        while vreader.isOpened():
            ret, frame = vreader.read()
            fnum += 1
            framestats = []
            if not frame is None:
                oframe = frame
                agenderx = self.agender.detect_genders_ages(frame)
                faces = []
                for face in agenderx:
                    # print("Faces: {}, {} {} {}".format(face['left'], face['top'], face['width'], face['height']))
                    age = int(math.ceil(face['age'] / 1.0))
                    gender = 'M'
                    if face['gender'] >= 0.3:
                        gender = 'F'
                    faces.append({'age': age, 'gender': gender, 'agender': face['gender'],
                                  'left': face['left'], 'right': face['right'],
                                  'top': face['top'], 'bottom': face['bottom']
                                  })

                    oframe = cv2.rectangle(oframe, (face['left'] - 5, face['top'] - 5),
                                           (face['left'] + face['width'] - 5, face['top'] + face['height'] - 5),
                                           (0, 255, 0),
                                           thickness=1)
                    if fnum % 2 == 0:
                        cv2.putText(oframe, '%s %s/%s' % (age, gender, face['gender']),
                                    (face['left'], face['top'] + face['height']),
                                    cv2.FONT_HERSHEY_SIMPLEX, 2, (0, 255, 0), 1)

                    vwriter.write(oframe)
                pframe = frame.copy()
                framedata = {'frame': framecount, 'faces': faces}
                # print("Framecount: %s" % (framecount))
                vidstats.append(framedata)
                framecount += 1
            else:
                print(" ====== End of Videofile =======")
                vreader.release()
                vwriter.release()
                print("Finished writing file")
                break
        # return results grpued

        return vidstats

    def analyze_frame(self, iframe=None, label=True, vwriter=None, publish=True):
        """
        Analyzes a video frame and generates age/gender data for the frame.
        It also generates an annotated version of the frame

        :param iframe: Input Cv2 RGB frame
        :param label: Boolean, whether to generate annotate information on the return video frame
        :param vwriter: Video Writer object handle to use to write the frame to disk

        :return: faces data, and an annotated frame
        """

        # Analyze Input Frame and returns dictionary or tuple
        agenderx = self.agender.detect_genders_ages(iframe)
        faces = []
        oframe = iframe.copy()
        dbdata = []
        for face in agenderx:
            # print("Faces: {}, {} {} {}".format(face['left'], face['top'], face['width'], face['height']))
            age = int(math.ceil(face['age'] / 1.0))
            gender = 'M'
            if face['gender'] >= 0.3:
                gender = 'F'
            agex = '0-10'
            faces.append({'age': age, 'agex' : agex, 'gender': gender, 'agender': face['gender'],
                          'left': face['left'], 'right': face['right'],
                          'top': face['top'], 'bottom': face['bottom']
                          })

            dbdata.append({'age': age, 'agex' : agex, 'gender': gender,
                          'left': face['left'], 'right': face['right'],
                          'top': face['top'], 'bottom': face['bottom']
                          })
            oframe = cv2.rectangle(oframe, (face['left'] - 5, face['top'] - 5),
                                   (face['left'] + face['width'] - 5, face['top'] + face['height'] - 5),
                                   (0, 255, 0),
                                   thickness=1)
            if label:
                cv2.putText(oframe, '%s %s' % (age, gender),
                            (face['left'], face['top'] + face['height']),
                            cv2.FONT_HERSHEY_SIMPLEX, 2, (0, 255, 0), 1)

            if vwriter:
                vwriter.write(oframe)

        framedata = {'frame': oframe, 'agender': faces, 'dbdata' : dbdata}

        return framedata

    def analyze_livevideo(self, camera=0, ofile=None, droprate=2, cameraid=None, location=None, publish=True):
        """
        Analyzes LiveVideo feeds unitl 'ESC' Key is pressed
        :param camera:
        :return:
        """
        # Use Camera#0 by default otherwise use the open specified
        cameradev = 0
        if camera:
            cameradev = camera

        vcamera = cv2.VideoCapture(cameradev)
        vstarttime = self.epoch()
        livefeed = True
        fcount = 0

        while livefeed:
            ret, frame = vcamera.read()
            oframe = frame.copy()
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # print("FrameNum: {}".format(fcount))
            if fcount % droprate == 0:
                print(" Analyzing Frame: {}".format(fcount))
                faceinfo = self.analyze_frame(iframe=frame, label=True, publish=True)
                cv2.imshow('Agender', faceinfo['frame'])
                if publish:
                    finfo = {}
                    finfo['cameraid'] = cameraid
                    finfo['location'] = location
                    finfo['agender'] = faceinfo['dbdata']
                    finfo['epoch'] = self.epoch()
                    dtstr = str(datetime.datetime.now()).split('.')[0]
                    dtdstr = dtstr.split(" ")
                    finfo['datetime'] = {'date': dtdstr[0], 'time': dtdstr[1]}
                    if len(faceinfo['dbdata']):
                        #print(json.dumps(finfo))
                        self.publishstats(stats=finfo)

            fcount += 1
            if cv2.waitKey(5) == 27:  # ESC key press
                livefeed = False

    echotopic = False
    def publishstats(self, topic=None, stats=None):
        """
        Handles publishing stats to database
        :return:
        """
        ltopic = self.msgtopic
        if topic:
            ltopic = topic
        if not self.echotopic:
            self.echotopic = True
            logging.info("Selecting MQTT topic {}".format(ltopic))

        self.mqttc.publish(ltopic, json.dumps(stats))

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

    ag = VultusCore(cascadePath=capth, msgtopic='camerastats')
    #ag.process_frame(imgfile='/Users/navendusinha/Downloads/akshil-01.jpg',
    #                 statticdir=os.getcwd())

    #ag.process_frame(imgfile='/Users/navendusinha/Downloads/akshil-02.jpg',
    #                 statticdir=os.getcwd())

    st = ag.epoch()
    loc= ag.getLocation()
    print("Location: {}".format(loc))
    ag.analyze_livevideo(cameraid='001', location=ag.getLocation())

    #ag.analyze_livevideo(droprate=3, cameraid='000', location=)
    # vstats = ag.analyze_video_file(vidfile='/Users/navendusinha/Downloads/IMG_6415.mp4')
    # vstats = ag.analyze_video_file(vidfile='/Users/navendusinha/Downloads/VID_20200110_193348.mp4')
    # vstats = ag.analyze_video_file(vidfile='/Users/navendusinha/Downloads/IMG_0209.mp4')
    # vstats = ag.analyze_video_file(vidfile='/Users/navendusinha/Library/Mobile Documents/com~apple~CloudDocs/adX/1581134750.mp4')
    # vstats = ag.analyze_video_file(vidfile='/Users/navendusinha/Documents/Zoom/2020-03-04/zoom_2.mp4')
    # en = ag.epoch()
    # print("Processing time: %s second(s)" % int((en - st) / 1000))
    # for vstat in vstats:
    #    print(vstat)
