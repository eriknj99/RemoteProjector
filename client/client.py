import os
import socket
from time import sleep
import socket


SEPARATOR = "<SEP>"
BUFFER_SIZE = 4096



HOST = '192.168.67.222'
PORT = 5002        

# A data class for keeping track of device progress
class deviceStatus:

    def to_string(self):
        out  = ""
        out += f"Device:{self.device}\n"
        out += f"Frame:{self.frame}\n"
        out += f"Tile:{self.tile_part} / {self.tile_whole}\n"
        out += f"Sample:{self.sample_part} / {self.sample_whole}\n"
        out += f"Done:{self.done}\n"
        return out

    def __init__(self,device,frame, tilePart, tileWhole, samplePart, sampleWhole):
        self.device = device
        self.frame = frame
        self.tile_part = tilePart
        self.tile_whole = tileWhole
        self.sample_part = samplePart
        self.sample_whole = sampleWhole
        self.done = False


def send_file(filename):
    filesize = os.path.getsize(filename)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(f"send_file{SEPARATOR}{filename}{SEPARATOR}{filesize}".encode())
        data = s.recv(BUFFER_SIZE)
        print('Received', repr(data))
        with open(filename, "rb") as f:
            while True:
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    break
                s.sendall(bytes_read)
    s.close()

def recieve_file(filename):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(f"recieve_file{SEPARATOR}{filename}".encode())
        with open(filename, "wb") as f:
            while True:
                bytes_read = s.recv(BUFFER_SIZE)
                if not bytes_read:
                    break
                f.write(bytes_read)
        print(f"File Saved: {filename}")

def get_info():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(f"get_info".encode())
        data = s.recv(BUFFER_SIZE)
        print(data.decode())
        s.close()

def new_job():
     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(f"new_job{SEPARATOR}JOBID{SEPARATOR}test.blend{SEPARATOR}input.png{SEPARATOR}0000.png".encode())
            data = s.recv(BUFFER_SIZE)
            print(data.decode())
            s.close()

def render():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(f"render".encode())
            data = s.recv(BUFFER_SIZE)
            print(data.decode())

            s.close()

def get_render_status():
    out = deviceStatus("NONE", 0, 0, 0, 0, 0)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(f"get_status".encode())
            data = s.recv(BUFFER_SIZE)
            data = data.decode()
            s.close()

            

            data = data.split("\n")
            for line in data:
                sectors = line.split(":")
                if(sectors[0] == "device"):
                    out.device = sectors[1]
                if(sectors[0] == "frame"):
                    out.frame = int(sectors[1])
                if(sectors[0] == "tile_part"):
                    out.tile_part = int(sectors[1])
                if(sectors[0] == "tile_whole"):
                    out.tile_whole = int(sectors[1])
                if(sectors[0] == "sample_part"):
                    out.sample_part = int(sectors[1])
                if(sectors[0] == "sample_whole"):
                    out.sample_whole = int(sectors[1])
                if(sectors[0] == "done"):
                    out.done = bool(sectors[1] == "True")
    return out


get_info()
sleep(.1)
new_job()
sleep(.1)
send_file("test.blend")
sleep(.1)
send_file("input.png")
sleep(.1)
render()
sleep(1)
while True:
    sleep(1)
    status = get_render_status()
    #print('\033c')
    print(status.to_string())
    if(status.done == True):
        break
    
sleep(.1)
recieve_file("0000.png")
