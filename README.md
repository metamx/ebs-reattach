Spot Termination EBS Reattachment
---------------------------------

This repository provides a salt-state for a service which will attempt to cleanly detach ebs volumes on spot termination, and cleanly mount or create them on instance launch.

Getting Started
=================
1. Define a pool of volumes for each cluster.
1. Tag all of the EBS volume in your pool the `pool` tag
with a unique name.
1. Add ebs_start.py and a configuration with at least pool name, volume count, size, and type
to start when your instance starts up.

Configuration
=============
ebs_start.py looks in `/etc/ebs/config.yaml` for settings in a YAML file.

```
pool_name: unique pool name
ebs: #EBS section
  size: volume size in GB
  type: EBS type: standard|gp2|io1|st1|sc1
  count: number of volumes. 0 to exit without doing anything
  filesystem_type: fs type. ext4, btrfs, and xfs are supported for fs status check before mount
  filesystem_args: fs creation arguments
  mountpoint: /ebs
  mount_args: mount arguments
  strict: If False, create volumes if there aren't enough available. If True, fail if anything goes wrong
  volume_ids: [list of volume ids to reattach]
tagfile: path to yaml file with tags to apply in the `tags` key. Useful if you have another system for tagging your instances.
cachefile: path to file that will hold volumes attached by this system. Useful if you have another system for handling ephemeral and ebs volumes.
ignore_tags: [list of tags from tagfile to ignore when tagging volumes]
tags: {dict of extra tags you want to add to volumes}
```

IAM Permissions
===============
ER needs the following permissions:
* ec2:CreateVolume
* ec2:AttachVolume
* ec2:DetachVolume
* ec2:CreateTags
* ec2:DescribeVolumes
* ec2:DescribeVolumeStatus
* ec2:DescribeVolumeAttribute
* ec2:DescribeTags

It supports any IAM authentication method that Boto uses.

Startup
=======
When ebs_start.py starts up, it gets a list of all volumes in the same AZ as the
instance, with the matching pool tag, and with a status of “available”. ER loops through the
available volumes and attempts to attach, tag, quickly verify the filesystem status, and mount as
many volumes as are desired in the configuration. If no existing volumes are suitable, then it will
create volumes, new filesystems, and mount them.

If it has any problems checking or mounting the filesystem, it will add a `blacklist` tag with an
error message. It will never attach any volumes with the `blacklist` tag.

Shutdown
========
ebs_stop.py kills all processes accessing mounted ebs volumes, then unmounts and detaches them.

This should be run at shutdown and when the Spot Termination notice is detected.

# Special Thanks
Special thanks to the original authors whose commits were clobbered in the OSS port.
* https://github.com/jstrunk
* https://github.com/himadrisingh001
