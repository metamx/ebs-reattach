#!/usr/bin/env python3

#   Copyright 2016 Metamarkets Group, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import boto
import boto.ec2
import boto.exception
import boto.utils
import subprocess
import shlex
import logging

import time

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

mounts = {} #keys are devices, values are mountpoints
with open("/etc/mtab", 'r') as mtab:
    for line in mtab.readlines():
        m = line.split(' ')
        mounts[m[0]] = m[1]

ec2_connection = boto.ec2.connect_to_region("us-east-1")

id = boto.utils.get_instance_metadata()["instance-id"]

count = 0
attached_volumes = None
while count < 10:
    try:
        attached_volumes = ec2_connection.get_all_volumes(filters={"attachment.instance-id":id})
        break
    except boto.exception.BotoServerError as e:
        count += 1
        logging.error("Failed to get volumes retrying {0}: {1}" .format(count, str(e)))
        time.sleep(5)

if count == 10 and attached_volumes == None:
    raise Exception("Error retreiving volumes !!")

for volume in attached_volumes:
    if volume.attach_data.device != "/dev/sda1":
        device = volume.attach_data.device.replace('/dev/sd', '/dev/xvd')
        if not device in mounts:
            continue
        logging.debug("killing all processes using {0}".format(mounts[device]))
        process = subprocess.Popen(shlex.split("fuser -v -k -m {0}".format(
            mounts[device])),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   universal_newlines=True)
        process.wait()
        if process.returncode not in (0,1):
            logging.error("{0}: failed to kill processes using {1}".format(
                volume.id,
                mounts[device]))
            logging.debug("{0}: {1}".format(process.returncode,
                                            process.communicate()[0]))

        logging.debug("unmounting {0} from {1}".format(volume.id, device))
        process = subprocess.Popen(shlex.split("umount {0}".format(device)),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   universal_newlines=True)
        process.wait()
        if process.returncode != 0:
            logging.error("failed to unmount {0}: {1}".format(volume.id,
                           process.communicate()[0]))
            process = subprocess.Popen(shlex.split("fuser -v -m {0}".format(
                mounts[device])),
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True)
            process.wait()
            logging.debug(process.communicate()[0])
        else:
            logging.info("unmounted {0}".format(volume.id))
        logging.debug("detaching {0}".format(volume.id))
        volume.detach()
        logging.info("detached {0}".format(volume.id))

ec2_connection.close()
