#!/usr/bin/python3
import threading
import time
import random
import os
import sys
import subprocess
from subprocess import PIPE, Popen
import shlex
import cv2 as cv
import numpy as np
from time import sleep


# A data class for keeping track of device progress
class deviceStatus:

    def to_string(self):
        out  = ""
        out += f"device:{self.device}\n"
        out += f"frame:{self.frame}\n"
        out += f"tile_part:{self.tilePart}\n"
        out += f"tile_whole:{self.tileWhole}\n"
        out += f"sample_part:{self.samplePart}\n"
        out += f"sample_whole:{self.sampleWhole}\n"
        out += f"done:{self.done}\n"
        return out

    def __init__(self,device,frame, tilePart, tileWhole, samplePart, sampleWhole):
        self.device = device
        self.frame = frame
        self.tilePart = tilePart
        self.tileWhole = tileWhole
        self.samplePart = samplePart
        self.sampleWhole = sampleWhole
        
        self.startTime = 0 
        self.completedFrames = []
        self.frameTimes = []

        self.done = False

# Parse the output from the blender command
def parseOutput(device, line, status):
    if("Rendered" in line):
        # Parse Rendered Tiles
        tileRatio = (line[line.index("Rendered") + len("Rendered"):line.index("Tiles")])
        tilePart = int(tileRatio[:tileRatio.index("/")])
        tileWhole = int(tileRatio[tileRatio.index("/") + 1:])
        
        # Parse Frame
        frame = int(line[line.index("Fra:") + len("Fra:"): line.index("Mem")])

        # Parse samples
        samplePart = 0 # Set default values in case they arn't displayed
        sampleWhole = 0
        if("Sample" in line):
            sampleSubStr = line[line.index("Sample"):]
            sampleRatio = ( sampleSubStr[len("Sample"):])

            if("," in sampleRatio):
                sampleRatio = sampleRatio[:sampleRatio.index(",")]
            if("\\n" in sampleRatio):
                sampleRatio = sampleRatio[:sampleRatio.index("\\n")]

            samplePart = int(sampleRatio[:sampleRatio.index("/")])
            sampleWhole = int(sampleRatio[sampleRatio.index("/") + 1:])

            # Update Status
            status.frame = frame
            status.tilePart = tilePart
            status.tileWhole = tileWhole
            status.samplePart = samplePart
            if(sampleWhole != 0):
                status.sampleWhole = sampleWhole

def get_blender_version(blender_exe)->str:
    cmd = f"{blender_exe} --version".split(" ")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    output, error = process.communicate()
    output = output.decode("utf-8").split("\n")
    output = output[0].split(" ")
    return output[1]

def render(blendFile, blender_exe, device, status, frame):
        # Get the next available frame and reserve it 
        nextFrame = frame
               
        # Reset the device status 
        status.startTime = time.time()
        status.tilePart = 0
        status.samplePart = 0
        status.frame = nextFrame
        
        # Generate the blender command
        cmd = f"{blender_exe} -b {blendFile} -f {nextFrame} -- --cycles-device {device} --cycles-print-stats"
        
        # Execute the blender command
        p = Popen(cmd, stderr=subprocess.DEVNULL, stdout=PIPE, shell=True)
        
        # Capture and parse the stdout
        while(p.poll() == None):
            line = str(p.stdout.readline())
            if("Rendered" in line):
                parseOutput(device,line, status)
       
        # Wait for process to end just in case
        p.wait()

        # Update the frame startTime and completedFrames
        status.frameTimes.append(time.time() - status.startTime)
        status.completedFrames.append(nextFrame)

        status.done = True
        return


class Projector:

    def project(self, blendFile):
        outputFile = f"0000.png"

        # Start the render thread with a new status
        self.status = deviceStatus(self.device,0,0,0,0,0)
        t = threading.Thread(target=render, args=(blendFile, self.blender_exe, self.device,self.status, 0,))
        t.start()

    def get_status(self)->deviceStatus:
        return self.status

    def __init__(self, blender_exe="blender", device="CPU"):
        self.blender_exe = blender_exe
        self.device = device
        self.blender_version = get_blender_version(self.blender_exe)
        self.status = deviceStatus(self.device, 0, 0, 0, 0, 0)

