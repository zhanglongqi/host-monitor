__author__ = 'longqi'
import socket
from subprocess import check_output
import time


address = ('155.69.214.102', 31500)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

while True:
    # msg = raw_input()
    ip = check_output(['ip', 'route'])
    # print(type(ip))
    hostname = socket.gethostname()
    #print(type(hostname))
    #time_now = time.strftime("%c")
    time_now = time.time()
    #print(type(time_now))
    msg = hostname + '##' + str(time_now) + '##' + ip.decode('utf-8')
    #print(type(msg))

    if not msg:
        break
    try:
        s.sendto(msg.encode('utf-8'), address)
        time.sleep(10)
    except socket.error:
        time.sleep(10)
        continue
s.close()
