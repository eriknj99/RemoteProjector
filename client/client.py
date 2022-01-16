import os
import shutil
import socket
from time import sleep
import socket
import log
from termcolor import colored
import json

SEPARATOR = "<SEP>"
BUFFER_SIZE = 4096
HOLD_TIME = 0.1 # Sleep time in seconds before sending a query


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

class NodeInfo:

    def to_string(self):
        out = ""
        out += f"{colored('Name', 'magenta')}        : {self.name}\n"
        out += f"{colored('Device', 'magenta')}      : {self.device}\n"
        out += f"{colored('Blender EXE', 'magenta')} : {self.blender_exe}\n"
        out += f"{colored('Blender Ver', 'magenta')} : {self.blender_ver}\n"
        out += f"{colored('Host', 'magenta')}        : {self.host}\n"
        out += f"{colored('Port', 'magenta')}        : {self.port}\n"
        return out


    def __init__(self, info_string):
        self.connected = False
        self.name = "NONE"
        info_string = info_string.split("\n")
        for line in info_string:
            sec = line.split(":")
            if(sec[0] == "name"):
                self.name = sec[1]
                self.connected = True
            if(sec[0] == "device"):
                self.device = sec[1]
            if(sec[0] == "blender_exe"):
                self.blender_exe = sec[1]
            if(sec[0] == "blender_ver"):
                self.blender_ver = sec[1]
            if(sec[0] == "host"):
                self.host = sec[1]
            if(sec[0] == "port"):
                self.port = sec[1]
        

# A class for communication with a node
class NodeHandler:
    def send_file(self,source_file,destination_file):
        sleep(HOLD_TIME)
        filesize = os.path.getsize(source_file)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.PORT))
            s.sendall(f"send_file{SEPARATOR}{destination_file}{SEPARATOR}{filesize}".encode())
            data = s.recv(BUFFER_SIZE)
            with open(source_file, "rb") as f:
                while True:
                    bytes_read = f.read(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    s.sendall(bytes_read)
        s.close()

    def recieve_file(self,source_file, destination_file):
        sleep(HOLD_TIME)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.PORT))
            s.sendall(f"recieve_file{SEPARATOR}{source_file}".encode())
            with open(destination_file, "wb") as f:
                while True:
                    bytes_read = s.recv(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    f.write(bytes_read)

    def get_info(self):
        try:
            sleep(HOLD_TIME)
            data = ""
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, self.PORT))
                s.sendall(f"get_info".encode())
                data = s.recv(BUFFER_SIZE) 
                s.close()
            return data.decode()
        except:
            return ""
            

    def new_job(self):
         try:
            sleep(HOLD_TIME)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.HOST, self.PORT))
                    s.sendall(f"new_job{SEPARATOR}{self.job_id}{SEPARATOR}{self.blend_file}{SEPARATOR}{self.input_image}{SEPARATOR}{self.output_image}".encode())
                    data = s.recv(BUFFER_SIZE).decode()
                    s.close()
                    if(data == "success"):
                        return True
                    return False
         except:
            return False

    def render(self):
        sleep(HOLD_TIME)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, self.PORT))
                s.sendall(f"render".encode())
                data = s.recv(BUFFER_SIZE)
                print(data.decode())
                s.close()

    def get_render_status(self):
        sleep(HOLD_TIME)
        out = deviceStatus("NONE", 0, 0, 0, 0, 0)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.HOST, self.PORT))
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
    
    def begin_render(self, source_file, destination_file):
        self.current_dest = destination_file
        log.debug("Starting Render")
        self.send_file(source_file, self.input_image)
        self.render()

    def is_render_complete(self)->bool:
        return self.get_render_status().done

    def end_render(self):
        if(self.current_dest != None):
            log.debug("Ending Render")
            self.recieve_file(self.output_image, self.current_dest)

    def check_connection(self)->bool:
        self.node = NodeInfo(self.get_info())
        return self.node.connected

    def __init__(self, host, port, job_id, blend_file, input_image, output_image):
        self.HOST = host
        self.PORT = port
        self.job_id = job_id
        self.blend_file = blend_file
        self.input_image = input_image
        self.output_image = output_image
        self.current_dest = None
        if(self.check_connection()):
            log.debug(f"Connected to {self.node.name}@{self.HOST}:{self.PORT}")
        else:
            log.error(f"Could not connect to node {self.HOST}:{self.PORT}")
            return

        # Start a new job to clear the workspace directory on the node
        if(not self.new_job()):
            log.error(f"Unable to start job")
            return

        # Send the blender file to the node
        self.send_file(blend_file, blend_file)

        print(self.node.to_string())


def read_config():
    out = {}
    f = open("config.json")
    data = json.load(f)["nodes"]
    for i in range(len(data)):
        out[data[i][0]] = data[i][1]
    return out 

 
# Clear the given directory including sub dirs 
def clear_dir(dir:str):
    for f in os.listdir(dir):
        path = os.path.join(dir, f) 
        if(os.path.isfile(path)):
            os.unlink(path)
        if(os.path.isdir(path)):
            shutil.rmtree(path)


def init_rendered_dir(cache_dir, generated):
    ren_dir = os.path.join(cache_dir, "rendered")
    clear_dir(ren_dir)
    for dir in generated.keys():
        new_dir = os.path.join(ren_dir, dir)
        os.mkdir(new_dir)

def get_generated(cache_dir):
    out = {}
    gen_dir = os.path.join(cache_dir, "generated")
    subdirs = [f for f in os.listdir(gen_dir) if os.path.isdir(os.path.join(gen_dir, f))]
    for sd in subdirs:
        img_dir = os.path.join(gen_dir, sd)
        out[sd] = [f for f in os.listdir(img_dir) if os.path.isfile(os.path.join(img_dir, f))]
    return out

def get_blend(cache_dir):
    files = [f for f in os.listdir(cache_dir) if os.path.isfile(os.path.join(cache_dir, f))]
    for f in files:
        if(".blend" in f and ".blend1" not in f):
            return os.path.join(cache_dir, f)
    return None

def parse_cache_name(cache_dir)->str:
    return os.path.basename(cache_dir)

def build_render_queue(cache_dir):
    out = {}
    gen_dir = os.path.join(cache_dir, "generated")
    subdirs = [f for f in os.listdir(gen_dir) if os.path.isdir(os.path.join(gen_dir, f))]
    for sd in subdirs:
        img_dir = os.path.join(gen_dir, sd)
        imgs = os.listdir(img_dir)
        for img in imgs:
            gen_img_path = os.path.join(img_dir, img)
            ren_img_path = gen_img_path.replace("generated", "rendered")
            out[gen_img_path] = ren_img_path
    return out

def project(cache_dir, input_image="input.png", output_image="0000.png"):
    log.debug("Scanning cache dir...")
    gen_structure = get_generated(cache_dir)
     
    init_rendered_dir(cache_dir, gen_structure)
    blend_file = get_blend(cache_dir)
    if(blend_file == None):
        log.error("No blender files found")
    
    render_queue = build_render_queue(cache_dir)
    
    log.debug("Initializing nodes...")
    node_configs = read_config()
    nodes = []
    for nc in node_configs.keys():
        nodes.append(NodeHandler(nc, node_configs[nc], parse_cache_name(cache_dir), blend_file, input_image, output_image))
    
    log.debug("Starting render process...")

    for node in nodes:
        input = list(render_queue.keys())[0]
        output = render_queue.pop(input)
        node.begin_render(input, output)
    
    while(len(render_queue) != 0):
        for node in nodes:
            if(node.is_render_complete()):
                node.end_render()
                input = list(render_queue.keys())[0]
                output = render_queue.pop(input)
                node.begin_render(input, output)
        sleep(1)

    



cache_dir = "/home/erik/Projects/Neon2/Neon/Cache/remote_test"
project(cache_dir)



#nh = NodeHandler("127.0.0.1", 5002, "TEST_JOB", "test.blend", "input.png", "0000.png")

#nh.begin_render("AAA.png")
#while(nh.is_render_complete() == False):
#    sleep(1)
#nh.end_render("output.png")

