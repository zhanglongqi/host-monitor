__author__ = 'longqi'
import socket
import sys

sys.path.append(".")
import db_cassandra

address = ('155.69.214.102', 31500)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(address)

db_cassandra.init_db()


def simplify_ip_info(source):
    temp = []
    target = ''
    source = source.split(' ')
    print('source: ')
    print(source)

    for i in range(0, len(source)):
        if source[i] == 'src':
            temp.append(source[i + 1])

    for t1 in temp:
        target = target + t1 + ' ; '
    return target


while True:
    data, addr = s.recvfrom(2048)
    if not data:
        print("client has exist")
        break
    # print "received:", data, "from", addr
    msg = str(data, 'utf-8').split('##')
    hostname = msg[0]
    time_rec = msg[1]
    ip = msg[2]
    ip = simplify_ip_info(ip)
    print(hostname, '-', time_rec, '-', ip)
    db_cassandra.update_host_info(hostname, time_rec, ip)

s.close()
