#!/usr/bin/python

import logging
import boto3
import requests
from botocore.exceptions import ClientError
import zipfile
import os


class UploadToS3:

    def __init__(self):
        self.zipdir = './temp/zipped/'
        self.unzipdir = './temp/unzipped/'
        self.s3dir = 'mort'

    def upload_file(self, file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = self.s3dir + file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(self.unzipdir + file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_from_ftp(self, file_name):

        from ftplib import FTP
        ftp = FTP('ftp.cdc.gov')  # connect to host, default port
        ftp.login()  # user anonymous, passwd anonymous@
        ftp.cwd('pub/Health_Statistics/NCHS/Datasets/DVS/mortality/')
        #dir_list = ftp.retrlines('LIST')  # list directory contents

        command = 'RETR ' + file_name
        with open(self.zipdir + file_name, 'wb') as fp:
            ftp.retrbinary(command, fp.write)
            print('Downloading ' + file_name)
        ftp.quit()

        return

    def main(self):
        bucket = 'data-engineer.club'
        #for year in range(1968, 1969):
        #    file_name = 'mort' + str(year) + '.zip'

        file_name = '../scratch/mort2018ps.zip'
            # Download the zip file
        self.download_from_ftp(file_name)

            # Unzip the zip file
        with zipfile.ZipFile(self.zipdir + file_name, 'r') as zip_ref:
            zip_ref.extractall(self.unzipdir)
            print('Unzipping ' + file_name)

        unzipped_files = os.listdir(self.unzipdir)

            # Upload unzipped file to S3
        for unzipped_file in unzipped_files:

            self.upload_file(unzipped_file, bucket)
            print('Uploaded to S3: ' + unzipped_file)

        return


if __name__ == "__main__":
    uts = UploadToS3()
    uts.main()
