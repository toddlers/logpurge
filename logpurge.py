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
        parser.add_option("--cfg",action="store",default="config.yaml",
                dest="config",type="string",help="read options from config file")
        parser.add_option("--list",action="store_true",
                dest="flist",default=False,help="list all sections")
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

# load configuration from config file
def getConf(filename):
    options = open(filename,'r')
    filegroups = yaml.load(options)
    return filegroups

# create s3 connection
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

            # get bucket information
            b = s3.get_bucket(bucket)
            for sourceFile in sourceFiles:
                # remove path information from filename
                baseFileName = os.path.basename(sourceFile)
                # create new key in s3 with bucket name appended
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
        logger.info("Nothing to upload")


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
        logger.info("Nothing to delete !")
    if errors:
        for error in errors:
            logger.error(error)



def moveOldFiles(filesToMove,destination):
    errors = []
    if filesToMove:
        try:
            for f in filesToMove:
                # used shutil instead of os.copy() because os.copy()
                # will only copy not move
                shutil.move(f,destination)
                logger.info("Moved file : %s to %s",f,destination)
        except ( IOError,os.error) as why:
            errors.append((f,destination,str(why)))
        except OSError as why:
            errors.extend((src,dst,str(why)))
    else:
        logger.info("Nothing to move !")
    if errors:
        for error in errors:
            logger.error(error)




def getOldFiles(files,dateregex,maxage):
    oldfiles = []
    for f in files:
        if re.search(dateregex,f):
            deltatime = datetime.now() - timedelta(days = int(maxage))

            # get the last modified timestamp for file

            filemtime = datetime.fromtimestamp(path.getmtime(f))

            # check the age of the file
            if filemtime < deltatime:
                oldfiles.append(f)
    return oldfiles


def processFilegroup(fgroups,fgroup,instanceId,flist):
    access_key = fgroups['AWS_ACCESS_KEY']
    secret_key = fgroups['AWS_SECRET_ACCESS_KEY']
    if isinstance(fgroups[fgroup],dict):

        # We dont need to append keys to logfile

        if not re.search("AWS",fgroup):
            logger.info("Processing the file group %s",fgroup)

        # get the options for filegroup

        maxage = fgroups[fgroup]["upto"].split(" ")[0]
        fpath = fgroups[fgroup]["path"]
        dateregex = fgroups[fgroup]["dateregex"]
        filepat = fgroups[fgroup]["files"]
        files = glob.glob(fpath + '/' + filepat)
        oldFiles = getOldFiles(files,dateregex,maxage)
        actions = fgroups[fgroup]["action"]

        # if list option is set , return with file names for filegroup
        if flist:
            return oldFiles
        else:
        # If list option is not set , execute actions
            for action in actions:
                if action.lower() == "s3":
                    bucket = fgroups[fgroup]["bucket"]
                    try:
                        uploadToS3(access_key,secret_key,bucket,instanceId,oldFiles)
                    except IOError as (errnotrerror):
                        logger.error("Error uploading files to %s : %s",bucket, error)
                elif action.lower() == "delete":
                    deleteOldFiles(oldFiles)
                elif action.lower() == "move":
                    moveDest = fgroups[fgroup]["dest"]
                    moveOldFiles(oldFiles,moveDest)
                else:
                    logger.error("Action specified (%s) is not the valid action" %(action))



def main():
    now = time.strftime('%Y%m%d-%H%M%S')
    opts = GetOptions()
    if not opts.config:
        logger.error("Need configuration file")
        opts.print_help_exit()
    try:
        filegroups = getConf(opts.config)
    except IOError as (errno,strerror):
        logger.error("Error opening config file %s: %s, quitting",\
                opts.config,strerror)
        sys.exit(1)

    try:

        # get the instnace ID for uploading to s3
        instanceId = getInstanceId()
    except:
        logger.error("Error getting EC2 instance id, quitting")
        sys.exit(1)

    aws_access_key = filegroups['AWS_ACCESS_KEY']
    aws_secret_key = filegroups['AWS_SECRET_ACCESS_KEY']

    # testing s3 connection before doing anything

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


    # Check if we only have to list the files

    if opts.flist:
        for filegroup in filegroups:
            processedFiles = processFilegroup(filegroups,filegroup,instanceId,opts.flist)
            if processedFiles:
                pprint.pprint(processedFiles)
            else:
                if not re.search("AWS",filegroup):
                    logger.info("No files for action in %s", filegroup)

        sys.exit(0)


    # Process only a specific filegroup
    if opts.only:
        # get the filegroup name
        filegroup = opts.only
        processFilegroup(filegroups,filegroup,instanceId,opts.flist)
    else:
        # process all the filegroups in the config
        for filegroup in filegroups:
            processFilegroup(filegroups,filegroup,instanceId,opts.flist)

if __name__ == "__main__":
    logger = GetLogger().initialize_logger()
    main()
