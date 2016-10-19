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

import logging
import os
import re
import shlex
import subprocess
import sys

import boto
import boto.ec2
import boto.utils
import yaml
from retrying import retry
from random import shuffle

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

DEFAULTS = {
    "tagfile": "/etc/ebs/tagfile.yaml",
    "cachefile": "/var/cache/ebs_volumes",
    "ignore_tags": [ ],
    "tags": { },
    "pool_name": None,
    "ebs": {
        "size": 500,
        "type": "sc1",
        "count": 0,
        "filesystem_type": "ext4",
        "filesystem_args": "",
        "mountpoint": "/ebs",
        "mount_args": "",
        "strict": False,  # create volumes if there aren't enough available
        "volume_ids": []
    },
    "eni": {
        "subnet_id": {},
        "eni_id": None
    }
}


def context_merge(default, config):
    """recursively merge keys from config into default. dictionary keys get merged, lists get appended,
    and everything else gets copied. default is overwritten."""
    for key in config:
        if key in default:
            if isinstance(default[key], dict) and isinstance(config[key], dict):
                context_merge(default[key], config[key])
            elif isinstance(default[key], list) and isinstance(config[key], list):
                default[key].extend(config[key])
            else:
                default[key] = config[key]
        else:
            default[key] = config[key]


def retry_if_value_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return isinstance(exception, ValueError)


def retry_if_throttled(exception):
    logging.error("Retrying on exception {0}".format(exception))
    return ((isinstance(exception, boto.exception.EC2ResponseError)
             and exception.error_code == "RequestLimitExceeded") or
            (isinstance(exception, boto.exception.BotoServerError)
             and exception.error_code == "Throttling" or exception.error_code == "ServiceUnavailable"))


@retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
       retry_on_exception=retry_if_throttled)
def attach_volume(volume, device, context):
    try:
        logging.debug("Attaching {0} to {1} at {2}".format(volume.id, context["instance_id"], device))
        volume.attach(context["instance_id"], device)
        logging.info("Attached {0} to {1} at {2}".format(volume.id, context["instance_id"], device))
        return True
    except boto.exception.EC2ResponseError as e:
        if e.error_code == "RequestLimitExceeded":
            raise
        logging.error("Failed to attach {0} to {1} at {2}".format(volume.id, context["instance_id"], device))
        logging.debug(str(e))
        return False


def next_device(start):
    if os.path.exists(start):
        if start[-1] == 'z':
            logging.error("all block device names are taken")
            raise OSError("all block device names are taken")
        return next_device(start[0:-1] + chr(ord(start[-1]) + 1))
    else:
        return start


def next_mountpoint(start):
    if os.path.exists(start):
        index = int(re.search(r"[0-9]+$", start).group())
        nextindex = index + 1
        return next_mountpoint(start.replace(str(index), str(nextindex)))
    else:
        return start


@retry(wait_exponential_multiplier=1000, stop_max_delay=300000,
       retry_on_exception=retry_if_throttled)
@retry(wait_exponential_multiplier=1000, stop_max_delay=300000,
       retry_on_exception=retry_if_value_error)
def wait_for_volume_state(volume, state):
    volume.update()
    logging.debug("waiting for {0} state: {1}".format(volume.id, volume.attach_data.status))
    if state not in (volume.status, volume.attach_data.status):
        raise ValueError("waiting for {0}".format(volume.id))


def mkfs(volume, context):
    device = volume.attach_data.device
    logging.debug("mkfs.{0} {1} {2}".format(context["ebs"]["filesystem_type"],
                                            context["ebs"]["filesystem_args"],
                                            device))
    process = subprocess.Popen(shlex.split("mkfs.{0} {1} {2}".format(context["ebs"]["filesystem_type"],
                                                                     context["ebs"]["filesystem_args"],
                                                                     device)),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               universal_newlines=True)
    process.wait()
    if process.returncode == 0:
        logging.info("filesystem created on {0} on {1}".format(volume.id, context["instance_id"]))
        return True
    else:
        output = process.communicate()[0]
        logging.error("{0}: {1}".format(process.returncode, output))
        raise OSError("{0}: {1}".format(process.returncode, output))


def mount_volume(volume, context):
    device = volume.attach_data.device
    mountpoint = next_mountpoint(context["ebs"]["mountpoint"] + "0")
    logging.debug("Mounting {0} as {1} at {2}".format(volume.id, device, mountpoint))
    os.mkdir(mountpoint)
    if int(re.search(r"[0-9]+$", mountpoint).group()) == 0:
        # e.g. /ebs -> /ebs0
        try:
            os.symlink(mountpoint, context["ebs"]["mountpoint"])
        except FileExistsError:
            pass
    if context["ebs"]["mount_args"] is not "":
        mount_args = "-o {0}".format(context["ebs"]["mount_args"])
    else:
        mount_args = ""
    logging.debug("mount {0} {1} {2}".format(mount_args,
                                             device,
                                             mountpoint))
    process = subprocess.Popen(shlex.split("mount {0} {1} {2}".format(mount_args,
                                                                      device,
                                                                      mountpoint)),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               universal_newlines=True)
    process.wait()
    if process.returncode == 0:
        logging.info("Mounted {0} at {1}".format(volume.id, device))
    else:
        logging.error("Failed to mount {0} at {1}".format(volume.id, device))
        logging.error("{0}: {1}".format(process.returncode, process.communicate()[0]))
        raise OSError("{0}: {1}".format(process.returncode, process.communicate()[0]))
    os.chmod(mountpoint, 0o1777)
    with open(context["cachefile"], 'a') as cache:
        cache.write("{0}\n".format(device))


@retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
       retry_on_exception=retry_if_throttled)
def get_volumes(context, volume_ids=[]):
    if len(volume_ids) == 0:
        ebs_volumes = context["ec2_connection"].get_all_volumes(
                filters={
                    "status": "available",
                    "tag:pool": context["pool_name"],
                    "availability-zone": context["az"]})
        logging.debug("found {0} volumes for pool {1}".format(len(ebs_volumes), context["pool_name"]))
    else:
        logging.debug("volumes_ids: {0}".format(volume_ids))
        ebs_volumes = context["ec2_connection"].get_all_volumes(volume_ids=volume_ids)
        logging.debug("found {0} volumes from list of {1}".format(len(ebs_volumes), len(volume_ids)))
    shuffle(ebs_volumes)
    return ebs_volumes


def create_volume(context):
    logging.debug("Creating volume")
    _create_volume = retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
                           retry_on_exception=retry_if_throttled)(context["ec2_connection"].create_volume)
    volume = _create_volume(context["ebs"]["size"], context["az"], volume_type=context["ebs"]["type"])
    logging.info("Created volume {0}".format(volume.id))
    wait_for_volume_state(volume, "available")
    return volume


@retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
       retry_on_exception=retry_if_throttled)
def blacklist(volume, message):
    logging.error("blacklisting {0} for {1}".format(volume.id, message))
    try:
        volume.add_tag("blacklist", message[:255])
    except boto.exception.EC2ResponseError as e:
        if e.error_code == "TagLimitExceeded":
            logging.error("Could not add blacklist tag {0}".format(volume.id))
        else:
            raise
    volume.detach()
    wait_for_volume_state(volume, "available")


def test_volume(volume, device, context):
    """run fsck for up to a minute. add blacklist tag on failure or timeout."""
    process = subprocess.Popen(shlex.split("blkid -s TYPE -o value {0}".format(device)),
                               stdout=subprocess.PIPE,
                               universal_newlines=True)
    process.wait()
    if process.returncode == 0:
        fstype = process.communicate()[0]
    else:
        output = process.communicate()[0]
        # blacklist and detach
        blacklist(volume, "no fs {0}: {1}".format(process.returncode,
                                                  output))
        logging.info("blkid found no fs on {0} {1}: {2}".format(volume.id, process.returncode, output))
        return False
    try:
        logging.debug("Running fsck on {0}".format(volume.id))
        if fstype == "xfs":
            process = subprocess.Popen(shlex.split("xfs_repair -n {0}".format(device)),
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True)
            process.wait(timeout=60)
        elif fstype == "btrfs":
            process = subprocess.Popen(shlex.split("btrfs-check --readonly {0}".format(device)),
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True)
            process.wait(timeout=60)
        else:
            process = subprocess.Popen(shlex.split("fsck -n {0}".format(device)),
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True)
            process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        output = process.communicate()[0]
        # blacklist and detach
        blacklist(volume, "timeout: {0}".format(output))
        logging.info("fsck failed {0} timeout: {1}".format(volume.id, output))
        return False
    if process.returncode != 0:
        output = process.communicate()[0]
        # blacklist and detach
        blacklist(volume, "{0}: {1}".format(process.returncode,
                                            output))
        logging.info("fsck failed {0} {1}: {2}".format(volume.id, process.returncode, output))
        return False
    else:
        return True


@retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
       retry_on_exception=retry_if_throttled)
def get_attached(context):
    return context["ec2_connection"].get_all_volumes(
            filters={"attachment.instance-id": context["instance_id"],
                     "tag:pool": context["pool_name"]})


def process_volumes(context):
    if len(context['ebs']['volume_ids']) == 0:
        count = context['ebs']['count']
        recovery = False
    else:
        logging.debug("recovering volumes: {0}".format(context['ebs']['volume_ids']))
        count = len(context['ebs']['volume_ids'])
        context['ebs']['strict'] = True
        recovery = True
    attached = get_attached(context)
    device = next_device("/dev/xvdf")
    while len(attached) < count:
        if len(context['ebs']['volume_ids']) == 0:
            volumes = get_volumes(context)
        else:
            volumes = get_volumes(context, volume_ids=context['ebs']['volume_ids'])
        try:
            volume = volumes.pop()
            test = True  # only fsck reattached volumes
        except IndexError:
            if recovery or context['ebs']['strict']:
                raise Exception("failed to attach volumes: {0}".format(
                        ", ".join(context['ebs']['volume_ids'])))
            else:
                volume = create_volume(context)
                test = False
        if (recovery == False) and ("blacklist" in volume.tags):
            continue
        if attach_volume(volume, device, context):
            wait_for_volume_state(volume, "attached")
            _volume_add_tags = retry(wait_exponential_multiplier=1000, stop_max_delay=60000,
                                     retry_on_exception=retry_if_throttled)(volume.add_tags)
            _volume_add_tags(context['tags'])
            logging.info("{0} tagged".format(volume.id))
            if test:
                if not test_volume(volume, device, context):
                    continue
            else:
                mkfs(volume, context)
            mount_volume(volume, context)
            logging.info("{0} mounted ".format(volume.id))
            device = next_device(device)
            attached.append(volume)
        elif not test:
            logging.error("failed to attach newly created {0}".format(volume.id))
            raise Exception("failed to attach newly created {0}".format(volume.id))
        else:
            "maybe another instance attached this volume first. try next one"
            pass


if __name__ == "__main__":
    # May want to take config file in as a parameter
    if os.path.isfile("./context.yaml"):
        config_file = "./context.yaml"
    elif os.path.isfile("/etc/ebs/config.yaml"):
        config_file = "/etc/ebs/config.yaml"
    else:
        logging.info("no config found exiting")
        sys.exit()

    with open(config_file, 'r') as file:
        config = yaml.load(file)

    context = DEFAULTS.copy()
    context_merge(context, config)

    metadata = boto.utils.get_instance_metadata()
    context["instance_id"] = metadata["instance-id"]
    context["az"] = metadata["placement"]["availability-zone"]
    context["ec2_connection"] = boto.ec2.connect_to_region(context["az"][0:-1])

    if os.path.isfile(context['tagfile']):
        with open(context['tagfile'], 'r') as file:
            tagconfig = yaml.load(file)

    context["tags"].update(tagconfig.get("tags", {}))
    context["tags"]["pool"] = context["pool_name"]
    context["tags"]["instance_id"] = context["instance_id"]

    for tag in context["ignore_tags"]:
        try:
            context["tags"].pop(tag)
        except KeyError:
            pass

    process_volumes(context)

    context["ec2_connection"].close()
