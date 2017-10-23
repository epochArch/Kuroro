import os,time
from socket import *
HOST='localhost'
PORT=17556
ADDR=(HOST,PORT)
COUNT=30
try:
    sock=socket(AF_INET,SOCK_STREAM)
    sock.connect(ADDR)
    sock.send('shutdown')
    sock.close()
except:
    print '\nproducer server donot start'
PORT=17557
ADDR1=(HOST,PORT)
try:
    sock1=socket(AF_INET,SOCK_STREAM)
    sock1.connect(ADDR1)
    sock1.send('shutdown')
    sock1.close()
except:
    print '\nleader server donot start'
PORT=17555
ADDR2=(HOST,PORT)
try:
    sock2=socket(AF_INET,SOCK_STREAM)
    sock2.connect(ADDR2)
    sock2.send('shutdown')
    sock2.close()
except:
    print '\nconsumer server donot start'
print 'shutdown broker server start...'
while COUNT:
  datanode=os.popen('ps -ef|grep 3997|grep -v "grep"|wc -l').read().strip()
  print "stopping .................................."
  time.sleep(1)
  COUNT = COUNT -1
  if datanode=='0':
        print "Kuroro stopped!"
        break