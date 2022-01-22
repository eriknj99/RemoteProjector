import os
import shutil
import socket
import time
from time import sleep
import socket
from termcolor import colored
import json

import bar
import log

SEPARATOR = "<SEP>"
BUFFER_SIZE = 4096
HOLD_TIME = 0.1 # Sleep time in seconds before sending a query

# Take a duration in seconds and convert it to a readable format
def formatTime(duration):
    if(duration < 60):
        return str(int(duration)) + "s"

    if(duration < 3600):
        return time.strftime('%M:%S', time.gmtime(int(duration)))

    return time.strftime('%H:%M:%S', time.gmtime(int(duration)))   

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
        color = 'magenta'
        gpu_color = 'blue'
        if(self.device == 'CUDA'):
            gpu_color = 'green'
        if(self.device == 'OPENCL'):
            gpu_color = 'red'

        out = ""
        out += f"{colored('Name', color)}        : {self.name}\n"
        out += f"{colored('Device', color)}      : {colored(self.device, gpu_color)}\n"
        out += f"{colored('Blender EXE', color)} : {self.blender_exe}\n"
        out += f"{colored('Blender Ver', color)} : {self.blender_ver}\n"
        out += f"{colored('Host', color)}        : {self.host}\n"
        out += f"{colored('Port', color)}        : {self.port}\n"
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
        self.send_file(source_file, self.input_image)
        self.render()
        self.render_start_time = time.time()

    def is_render_complete(self)->bool:
        return self.get_render_status().done

    def end_render(self):
        if(self.current_dest != None):
            
            self.total_time += time.time() - self.render_start_time
            self.total_rendered += 1
            
            self.recieve_file(self.output_image, self.current_dest)

    def check_connection(self)->bool:
        self.node = NodeInfo(self.get_info())
        return self.node.connected
    
    def to_string(self):
        color = 'magenta'
        self.node.host = self.HOST
        out = self.node.to_string()
        if(self.state == -1):
            out += f"{colored('Status', color)}      : {colored('Ready', 'green')}\n"
        elif(self.state == 0):
            out += f"{colored('Status', color)}      : {colored('Rendering', 'blue')}\n"
            out += f"{colored('Job', color)}         : {self.job_id}\n"
            file_path = self.current_dest.split("/")
            file_name = file_path[len(file_path) - 2:]
            out += f"{colored('File', color)}        : {file_name[0]}/{file_name[1]}\n"
            out += f"{colored('Time', color)}        : {formatTime(time.time() - self.render_start_time)}\n"
            
            out += f"{colored('Rendered', color)}    : {self.total_rendered} frames\n"
            if(self.total_rendered != 0):
                out += f"{colored('Avg time', color)}    : {formatTime(self.total_time / self.total_rendered)}\n"


        elif(self.state == 1):
            out += f"{colored('Status', color)}      : {colored('Idle', 'red')}\n"
            out += f"{colored('Rendered', color)}    : {self.total_rendered} frames\n"
            if(self.total_rendered != 0):
                out += f"{colored('Avg time', color)}    : {formatTime(self.total_time / self.total_rendered)}\n"

        return out

    def __init__(self, host, port, job_id, blend_file, input_image, output_image):
        self.HOST = host
        self.PORT = port
        self.job_id = job_id
        self.blend_file = blend_file
        self.input_image = input_image
        self.output_image = output_image
        self.current_dest = None
        
        self.total_rendered = 0
        self.total_time = 0
        self.render_start_time = -1

        self.state = -1
        if(not self.check_connection()):
            return

        # Start a new job to clear the workspace directory on the node
        if(not self.new_job()):
            log.error(f"Unable to start job")
            return

        # Send the blender file to the node
        self.send_file(blend_file, blend_file)

def print_status(completed_frames, total_frames, nodes):
    log.clear()

    for l in log.log_cache:
        print(l, end="")
    
    progress_bar = bar.drawGraph("Frames", "", completed_frames, 0, total_frames, color="blue", precision=0, numTicks = 3, length=-1)
    print(progress_bar)
    print('━' * 20)
    for node in nodes:
        print(node.to_string())
        print('━' * 20)

# Read a client config file as a json dict
def read_config():
    out = {}
    config_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.json")
    f = open(config_file_path)
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

# If the dir does not exist, create it, else delete its contents 
def init_output_dir(output_dir):
    if(os.path.isdir(output_dir)):
        clear_dir(output_dir)
    else:
        os.mkdir(new_dir)

# Get a dict of input image -> output image from 2 ordered arrays of input_dirs,output_dirs
def build_render_queue(input_dirs, output_dirs):
    out = {}
    for i in range(len(input_dirs)):
        source_files = ls_abs(input_dirs[i])
        for file in source_files:
            out[file] = os.path.join(output_dirs[i], os.path.basename(file))
    return out

# Get an array of absolute paths of files in a dir
def ls_abs(input_dir):
    file_paths = []
    for folder, subs, files in os.walk(input_dir):
        for filename in files:
            file_paths.append(os.path.abspath(os.path.join(folder, filename)))
    return file_paths

# Find the blender file in a directory
def get_blend(cache_dir):
    files = [f for f in os.listdir(cache_dir) if os.path.isfile(os.path.join(cache_dir, f))]
    for f in files:
        if(".blend" in f and ".blend1" not in f):
            return os.path.join(cache_dir, f)
    return None

# Get the cache name from the abs path of the cache 
def parse_cache_name(cache_dir)->str:
    return os.path.basename(cache_dir)


# job_id         : Does not matter, should be cache name
# input_folders  : A list of abs paths to directories containing images
# output_folders : A list of abs paths in the same order as input_folders where the output will be stored
# input_image    : The name of the image that the blend file is expecting
# output_image   : The name of the file that the blend file will output
def project(job_id, blend_file, input_folders, output_folders, input_image="input.png", output_image="0000.png"):
    
    # Init the given output directories
    for output_dir in output_folders:
        init_output_dir(output_dir)

    # Build the render queue from the input/output folders
    render_queue = build_render_queue(input_folders, output_folders)
    
    # Initialize the nodes from the config file
    node_configs = read_config()
    nodes = []
    for nc in node_configs.keys():
        nodes.append(NodeHandler(nc, node_configs[nc], job_id, blend_file, input_image, output_image))

   
    # Init vars to keep track of render progress
    total_frames = len(render_queue)
    completed_frames = 0
    
    # Dispatch all render jobs
    done = False
    while(not done):
        print_status(completed_frames, total_frames, nodes)
        done = True
        for node in nodes:
            # -1 : Node has not been given a render job yet
            if(node.state == -1):
                done = False
                input = list(render_queue.keys())[0]
                output = render_queue.pop(input)
                node.begin_render(input, output)
                node.state = 0
                

            # 0 : Node is currently processing jobs
            elif(node.state == 0):
                done = False
                if(node.is_render_complete()):
                    node.end_render()
                    completed_frames+=1
                    # When there are no more jobs set state to 1
                    if(len(render_queue) == 0):
                        node.state = 1
                    else:
                        input = list(render_queue.keys())[0]
                        output = render_queue.pop(input)
                        node.begin_render(input, output)

        sleep(1)
    

