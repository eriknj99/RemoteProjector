import socket
import os
import json
import shutil

import projector
import log

class Job:


    def get_info(self):
        return (f"\tID: {self.job_id}\n\tBlend File: {self.blend_file}\n\tInput Image: {self.input_image}\n\tOutput Image: {self.output_image}")

    def __init__(self, job_id, blend_file, input_image, output_image):
        self.job_id = job_id
        self.blend_file = blend_file 
        self.input_image = input_image
        self.output_image = output_image


class Node:
   
# --- Server Functions --- 
   
    # Get the server information
    def get_info(self) -> str:
        out = ""
        out += (f"name:{self.name}\n")
        out += (f"device:{self.device}\n")
        out += (f"blender_exe:{self.blender_exe}\n")
        out += (f"blender_ver:{self.projector.blender_version}\n")
        out += (f"host:{self.host}\n")
        out += (f"port:{self.port}\n")
        return out

    # Recieve a file and save it to the workspace dir
    def recieve_file(self, command, conn):
        # remove absolute path if there is
        filename = command[1]
        filesize = command[2]

        filename = "./workspace/" + os.path.basename(filename)

        filesize = int(filesize)
        
        with open(filename, "wb") as f:
            #f.write(received[3].encode())
            while True:
                bytes_read = conn.recv(self.BUFFER_SIZE)
                if not bytes_read:    
                    break
                f.write(bytes_read)
        log.info(f"File Recieved: {filename} , {filesize} B")

    # Clear the workspace directory and create a new job with the given params
    def new_job(self, command):
        # Clear the workspace directory
        self.clear_dir("./workspace")

        # Parse the command
        job_id       = command[1]
        blend_file   = command[2]
        input_image  = command[3]
        output_image = command[4]

        self.current_job = Job(job_id, blend_file, input_image, output_image)
        log.info("New Job Started\n" + self.current_job.get_info())


    # Render the current job in a seperate thread
    def render(self, conn):
        conn.sendall("Rendering".encode())
        self.projector.project("./workspace/" + os.path.basename(self.current_job.blend_file))
        log.info("Started Render")

    # Get the render status as a string
    def get_status(self)->str:
        status = self.projector.get_status()
        if(status.done):
            log.info("Render Complete")
        return status.to_string()
 
    # Send the requested file 
    def send_file(self, command, conn):
        filename = "./workspace/" + command[1]
        filesize = os.path.getsize(filename)
        #out = (f"Filename = {filename}\n")
        #out += (f"Filesize = {filesize}\n")
        #conn.sendall(out.encode())
        with open(filename, "rb") as f:
            while True:
                bytes_read = f.read(self.BUFFER_SIZE)
                if not bytes_read:
                    break
                conn.sendall(bytes_read)
        conn.close()
        log.info(f"File Transmitted: {filename}, {filesize} B")

    # The main server thread
    def run_server(self):
         while(True):
             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.host, self.port))
                s.listen()
                conn, addr = s.accept()
                with conn:
                    while True:
                        data = conn.recv(self.BUFFER_SIZE)
                        if not data:
                            break
                        command = data.decode().split(self.SEPARATOR)
                        #if(command[0] != "get_status"):
                        #    log.debug(f"Recieved {command[0]} from {addr}")

                        if(command[0] == "send_file"):
                            conn.sendall(b'ready')
                            self.recieve_file(command, conn)
                        elif(command[0] == "get_info"):
                            conn.sendall(self.get_info().encode())
                        elif(command[0] == "new_job"):
                            self.new_job(command)
                            conn.sendall("success".encode())
                        elif(command[0] == "render"):
                            self.render(conn)
                        elif(command[0] == "get_status"):
                            conn.sendall(self.get_status().encode())
                        elif(command[0] == "recieve_file"):
                            self.send_file(command, conn)
                            break
                        else:
                            conn.sendall(b'Command not found')

#   --- UTIL ---         
    # Clear the given directory including sub dirs 
    def clear_dir(self,dir:str):
        for f in os.listdir(dir):
            path = os.path.join(dir, f) 
            if(os.path.isfile(path)):
                os.unlink(path)
            if(os.path.isdir(path)):
                shutil.rmtree(path)

    # Read the node config file into instance variables 
    def read_config(self):
        f = open(self.config_file)
        data = json.load(f)
        self.name = data["name"]
        self.device = data["device"]
        self.blender_exe = data["blender_exe"]
        self.host = data["host"]
        self.port = data["port"]
    
    # Get the server information
    def log_info(self):
        log.info(f"Name:        {self.name}")
        log.info(f"Device:      {self.device}")
        log.info(f"Blender EXE: {self.blender_exe}")
        log.info(f"Blender Ver: {self.projector.blender_version}")
        log.info(f"Host:        {self.host}")
        log.info(f"Port:        {self.port}")
        log.debug(f"Buffer Size: {self.BUFFER_SIZE}")
        log.debug(f"Separator  : {self.SEPARATOR}")

    def __init__(self):
        self.config_file = "config.json"
        self.read_config()
        self.BUFFER_SIZE = 4096
        self.SEPARATOR = "<SEP>"
        self.projector = projector.Projector(self.blender_exe, self.device)
        self.log_info()
        self.current_job = None
        self.has_job = False
        self.run_server()



#projector.project("test.blend", blender_exe="./blender/blender")
Node()


