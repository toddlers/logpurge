# GHRH

Manage logs on the servers easily. I have written this after fiddling with logrotate utility on linux boxes. 
Some of the problems which I encountered with logorotate utility are :

- There is no regex support in logrotate utility. Only globs are available
- You have to restart your application to point to new log file.
- Managing multiple actions for your logs e.g. you want to upload your logs to s3 selectively and then delete them from the disk



## Install the dependecies before using :

``` sh
sudo yum install python-boto.noarch PyYAML.x86_64 python-httplib2.noarch -y
```

## Usage

### General help
``` sh
λ: python logpurge.py --help
Usage: logpurge.py [options]

Options:
  -h, --help    show this help message and exit
  --cfg=CONFIG  read options from config file
  --list        list all sections
  --only=ONLY   only process the specified section from the file
```


- Using only option you can process a specific filegroup from the config file
- List option will process all the sections in the config file and provide the files list for action

## Example config

```

AWS_ACCESS_KEY: 'AKIAIN4A4SHXQ'
AWS_SECRET_ACCESS_KEY: 'ALO5UUvZsSvbDn9kTqAGn9Z'

filegroup1:
    path: '/mnt/ephemeral/jetty/logs/'
    files: "server.log.*"
    dateregex: '(\d{4}-\d{2}-\d{2})'
    upto: "1 days ago"
    action:
        - s3
        - delete
    bucket: 'logs_testing'

filegroup2:
    path: '/mnt/ephemeral/jetty/logs/'
    files: "activity.log.*"
    dateregex: '(\d{4}-\d{2}-\d{2})'
    upto: "1 days ago"
    action:
        - s3
        - delete
    bucket: 'logs_testing'

```

- Three types of actions are supported 
  - S3 will upload your logs to the mentioned s3 bucket with structure are "bucket/instance_ID/logfilename"
  - DELETE will delete the logs from the disk
  - MOVE will the log files to the mentioned destination. It will create the destination if doesn't exists
  
- You need to provide the date regex which is in the log file e.g. 2014-03-12.start.log
- Provide file glob . e.g. for file name 2014-03-12.start.log , glob will be *.start.log
- Age is specified by "upto". Specify the maxage like "x days ago"
