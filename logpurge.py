#!/usr/bin/env python

from datetime import datetime,timedelta
from os import path
import optparse
import os
import sys
import yaml
import glob
import time
import re
import httplib2
import json
import logging
import boto
import pprint
import shutil


class GetOptions:
    def __init__(self):
        parser = optparse.OptionParser()
        parser.add_option("--simulate",action="store_true",
                dest="simulate",default=False,help="Don't take action")
        parser.add_option("--cfg",action="store",default="config.yaml",
                dest="config",type="string",help="read options from config file")
        parser.add_option("--list",action="store_true",
                dest="list",default=False,help="list all sections")
        parser.add_option("--only",action="store",
                dest="only",type="string",help="only process the specified section from the file")
        self.parser = parser
        self.parse()

    def parse(self):
        (self.options,self.args) = self.parser.parse_args()

    def __getattr__(self,k):
        return getattr(self.options,k)

    def print_help_exit(self):
        self.parser.print_help()
        exit(1)


class GetLogger:
    def initialize_logger(self):
        baseFileName = os.path.basename(__file__)
        logFileName = os.path.splitext(baseFileName)[0] + ".log"
        logging.basicConfig(
                filename = logFileName,
                level = logging.INFO,
                format = '%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                datefmt = '%H:%M:%S'
                )

        # set up logging to console
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)

        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')

        # add the handler to the root logger
        logging.getLogger('').addHandler(console)
        logger = logging.getLogger(__name__)
        return logger


def getInstanceId():
    h = httplib2.Http()

    # getting the instance id from ec2 metadata
    url = "http://169.254.169.254/latest/meta-data/instance-id"
    resp,content = h.request(url,"GET")

    # strip out the leading 'i-'

    match = re.search('i-(.+)',content)
    iid = match.group(1)
    return iid


def getConf(filename):
    options = open(filename,'r')
    filegroups = yaml.load(options)
    return filegroups

def getConnection(access,secret):
    conn = boto.connect_s3(access,secret)
    return conn

def testS3(access,secret,iid,now):
    bucket_name = iid
    s3 = getConnection(access,secret)
    bucket = s3.lookup(iid)
    if bucket:
        logger.error("Bucket (%s) already exists" % bucket_name)
    else:
        # let's try to create the bucket. This will fail if
        # the bucket has already been created by someone else
        try:
            bucket = s3.create_bucket(bucket_name)
            k = bucket.new_key("logging/test")
            k.set_contents_from_string("Testing S3 at " + now )
            k.delete()
            s3.delete_bucket(bucket_name)
        except s3.provider.storage_create_error, e:
            logger.error('Bucket (%s) is owned by another user' % bucket_name)



def uploadToS3(access,secret,bucket,iid,sourceFiles):
    s3 = getConnection(access,secret)
    if sourceFiles:
        try:
            b = s3.get_bucket(bucket)
            for sourceFile in sourceFiles:
                baseFileName = os.path.basename(sourceFile)
                key_name = os.path.join(iid,baseFileName)

                # Create a new object named sourceFile
                k = b.new_key(key_name)

                # Set the contents from sourceFile
                k.set_contents_from_filename(sourceFile)
                logger.info("Uploading file : %s to %s",sourceFile,bucket)
        except boto.exception.S3CreateError as (status, reason):
            logger.error("S3 Error creating %s in %s: %s",
                    key_name,bucket,reason)
        except boto.exception.S3PermissionsError as (reason):
            logger.error("S3 Error with permission on %s:%s: %s",
                    bucket,key_name,reason)
    else:
        print "Nothing to upload"


def deleteOldFiles(filesToDelete):
    errors = []
    if filesToDelete:
        for f in filesToDelete:
            try:
                os.remove(f)
                logger.info("Removed file : %s",f)
            except ( IOError,os.error) as why:
                errors.append((f,str(why)))
            except OSError as why:
                errors.extend((f,str(why)))
    else:
        print "Nothing to delete !"
    if errors:
        for error in errors:
            logger.error(error)

def moveOldFiles(filesToMove,destination):
    errors = []
    if filesToMove:
        try:
            for f in filesToMove:
                shutil.move(f,destination)
                logger.info("Moved file : %s to %s",f,destination)
        except ( IOError,os.error) as why:
            errors.append((f,destination,str(why)))
        except OSError as why:
            errors.extend((src,dst,str(why)))
    else:
        print "Nothing to move !"
    if errors:
        for error in errors:
            logger.error(error)




def getOldFiles(files,dateregex,maxage):
    oldfiles = []
    for f in files:
        if re.search(dateregex,f):
            deltatime = datetime.now() - timedelta(days = int(maxage))
            filemtime = datetime.fromtimestamp(path.getmtime(f))
            if filemtime < deltatime:
                oldfiles.append(f)
    return oldfiles


def main():
    logging.basicConfig(format="logpurge.py: %(message)s")
    now = time.strftime('%Y%m%d-%H%M%S')
    opts = GetOptions()
    if not opts.config:
        print "Need configuration file"
        opts.print_help_exit()
    try:
        filegroups = getConf(opts.config)
    except IOError as (errno,strerror):
        logger.error("Error opening config file %s: %s, quitting",\
                opts.config,strerror)
        sys.exit(1)

    try:
        instanceId = getInstanceId()
    except:
        logger.error("Error getting EC2 instance id, quitting")
        sys.exit(1)

    # testing s3 connection before doing anything
    aws_access_key = filegroups['AWS_ACCESS_KEY']
    aws_secret_key = filegroups['AWS_SECRET_ACCESS_KEY']
    try:
        testS3(aws_access_key,aws_secret_key,instanceId,now)
    except boto.exception.NoAuthHandlerFound:
        logger.error("S3 authentication error, quitting")
        sys.exit(2)
    except boto.exception.S3CreateError as (status, reason):
        logger.error("S3 Error creating %s:%s: %s, quitting",\
                instanceId, now + 'test', reason)
        sys.exit(2)
    except boto.exception.S3PermissionsError as (reason):
        logger.error("S3 Error with permissions on %s:%s: %s, quitting",\
                instanceId, instanceId + "-" + now + "-test", reason)
        sys.exit(2)
    except:
        logger.error("S3 unknown error, quitting")
        sys.exit(2)
    for filegroup in filegroups:
        if isinstance(filegroups[filegroup],dict):
            logger.info("Processing the file group %s", filegroup)
            maxage = filegroups[filegroup]["upto"].split(" ")[0]
            fpath = filegroups[filegroup]["path"]
            dateregex = filegroups[filegroup]["dateregex"]
            filepat = filegroups[filegroup]["files"]
            files = glob.glob(fpath + '/' + filepat)
            processedFiles = getOldFiles(files,dateregex,maxage)
            actions = filegroups[filegroup]["action"]
            for action in actions:
                if action.lower() == "s3" and isinstance(filegroups[filegroup],dict):
                    bucket = filegroups[filegroup]["bucket"]
                    try:
                        uploadToS3(aws_access_key,aws_secret_key,bucket,instanceId,processedFiles)
                    except IOError as (errno,strerror):
                        logger.error("Error uploading files to %s : %s",bucket, error)
                elif action.lower() == "delete" and isinstance(filegroups[filegroup],dict):
                    deleteOldFiles(processedFiles)
                elif action.lower() == "move" and isinstance(filegroups[filegroup],dict):
                    moveDest = filegroups[filegroup]["dest"]
                    moveOldFiles(processedFiles,moveDest)
                else:
                    print "Action specified (%s) is not the valid action" %(action)

if __name__ == "__main__":
    logger = GetLogger().initialize_logger()
    main()
