#!/usr/bin/python

import logging
import boto3
import requests
from botocore.exceptions import ClientError
import zipfile
import os
from ftplib import FTP


class UploadToS3:

    def __init__(self):
        self.zipdir = './temp/zipped/'
        self.unzipdir = './temp/unzipped/'
        self.s3dir = 'mort/'

        print('Creating temp directories')
        os.makedirs(self.zipdir, 0o777)
        os.makedirs(self.unzipdir, 0o777)

        # Open FTP connection
        self.ftp = FTP('ftp.cdc.gov')  # connect to host, default port
        self.ftp.login()  # user anonymous, passwd anonymous@
        self.ftp.cwd('pub/Health_Statistics/NCHS/Datasets/DVS/mortality/')


    def upload_file(self, file_name, bucket, object_name):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        print(object_name)

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(self.unzipdir + file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_from_ftp(self, file_name):
        command = 'RETR ' + file_name
        with open(self.zipdir + file_name, 'wb') as fp:
            print('Downloading: ' + file_name)
            self.ftp.retrbinary(command, fp.write)
        return

    def main(self, start_year, end_year):
        bucket = 'data-engineer.club'
        for year in range(start_year, end_year):      # End year excluded
            print('Processing year ' + str(year))
            file_name = 'mort' + str(year) + 'us.zip'

            # Download the zip file
            self.download_from_ftp(file_name)

            # Unzip the zip file and clean up
            with zipfile.ZipFile(self.zipdir + file_name, 'r') as zip_ref:
                print('Unzipping: ' + file_name)
                zip_ref.extractall(self.unzipdir)

            os.remove(self.zipdir + file_name)
            print('Deleted local copy of ' + self.zipdir + file_name)

            # Upload unzipped file to S3 and clean up
            unzipped_files = os.listdir(self.unzipdir)
            print(len(unzipped_files))
            target_name = self.s3dir + 'mort' + str(year) + '.txt'

            for unzipped_file in unzipped_files:
                print('Uploading to S3: ' + unzipped_file)
                self.upload_file(unzipped_file, bucket, target_name)

                os.remove(self.unzipdir + unzipped_file)
                print('Deleted local copy of ' + self.unzipdir + unzipped_file)

        self.ftp.quit()
        os.removedirs(self.zipdir)
        os.removedirs(self.unzipdir)
        return


if __name__ == "__main__":
    import sys

    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])

    uts = UploadToS3()
    uts.main(start_year, end_year)
