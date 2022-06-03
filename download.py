import os
import io
import sys
import math
import logging
import threading
import boto3
import botocore
from botocore.config import Config
from boto3.s3.transfer import (TransferConfig)
from datetime import (datetime, timedelta, timezone)


def get_logger(log_level="DEBUG"):
  logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s] %(message)s")
  logger = logging.getLogger(__name__)
  logger.setLevel(log_level)
  return logger


class ProgressPercentage(object):


    def __init__(self, filename, filesize):
        self._filename = filename
        self._size = filesize
        self._seen_so_far = 0
        self._lock = threading.Lock()


    def __call__(self, bytes_amount):
        def convertSize(size):
            if (size == 0):
                return '0B'
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            i = int(math.floor(math.log(size,1024)))
            p = math.pow(1024,i)
            s = round(size/p,2)
            return '%.2f %s' % (s,size_name[i])

        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)        " % (
                    self._filename, convertSize(self._seen_so_far), convertSize(self._size),
                    percentage))
            sys.stdout.flush()


class AWS_S3_Download:


  def __init__(self, logger, download_to_file, bucket_name, s3_file) -> None:
    self.logger = logger
    if not download_to_file or not bucket_name or not s3_file:
      err_msg = "Files or Download file or Bucket name is empty"
      self.logger.error(err_msg)
      raise Exception(err_msg)

    self.download_to_file = download_to_file
    self.bucket_name = bucket_name
    self.s3_file = s3_file

    my_config = Config(
      region_name = 'us-east-1',
      # signature_version = 'v2',
      retries = {
          'max_attempts': 3,
          'mode': 'standard'
      }
    )
    self.s3_client = boto3.client('s3', config=my_config)
    self.s3_res = boto3.resource('s3', config=my_config)


  def download_approach_1(self):
      s3_obj = self.s3_res.Bucket(self.bucket_name).Object(self.s3_file).get()
      with io.FileIO(self.download_to_file, 'wb') as file:
        for i in s3_obj['Body']:
            file.write(i)


  def download_approach_2a(self):
    self.s3_res.download_file(self.bucket_name, self.s3_file, self.download_to_file)


  def download_approach_2b(self):
    self.s3_client.download_file(Bucket=self.bucket_name, Key=self.s3_file, Filename=self.download_to_file)


  def download_approach_3(self):
    s3_obj = self.s3_res.Bucket(self.bucket_name).Object(self.s3_file).get()
    with io.FileIO(self.download_to_file, 'wb') as file:
      for i in s3_obj['Body']:
          file.write(i)


  def download_approach_4(self):
      s3_obj = self.s3_client.get_object(
        Bucket = self.bucket_name,
        Key = self.s3_file,
        ResponseContentType = "binary/octet-stream",
        ResponseContentDisposition = f"attachment; filename={self.s3_file}",
        ResponseContentLanguage = "en-US",
        ResponseCacheControl = 'No-cache',
        ResponseExpires = (datetime.now(timezone.utc)+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
      )

      with io.FileIO(self.download_to_file, 'wb') as file:
        for i in s3_obj['Body']:
            file.write(i)


  def download_approach_5(self):
    with open(self.download_to_file, "wb") as data:
      self.s3_client.download_fileobj(self.bucket_name, self.s3_file, data)


  def download_approach_interactive(self):
    self.s3_client.download_file(
        Bucket=self.bucket_name,
        Key=self.s3_file,
        Filename=self.download_to_file,
        Config=TransferConfig(
          max_concurrency=10,
          use_threads=True
        ),
        Callback=ProgressPercentage(self.download_to_file,
                                            (self.s3_client.head_object(
                                                Bucket=self.bucket_name,
                                                Key=self.s3_file))["ContentLength"])
      )


  def download_s3_file(self, callback):
    self.logger.info(f"Download S3:")
    self.logger.info(f"from bucket - {self.bucket_name}")
    self.logger.info(f"s3_file - {self.s3_file}, download to - {self.download_to_file}")
    self.logger.info(f"Start")
    try:
      callback()
    except botocore.exceptions.ClientError as error:
        self.logger.error(f"{error}")

    except botocore.exceptions.ParamValidationError as error:
        self.logger.error(f"{error}")
    finally:
      self.logger.info(f"Finish")


if __name__ == "__main__":
  pass
  # os.environ['AWS_ACCESS_KEY_ID'] =
  # os.environ['AWS_SECRET_ACCESS_KEY'] =
  # os.environ['S3_BUCKET_NAME'] =
  # os.environ['S3_KEY_FILE'] =
  # os.environ['DOWNLOAD_TO_FILE'] =

  logger = get_logger(os.getenv('LOG_LEVEL', 'DEBUG'))
  download_file = os.getenv('DOWNLOAD_TO_FILE')
  bucket_name = os.getenv('S3_BUCKET_NAME')
  s3_key = os.getenv('S3_KEY_FILE')
  aws_s3 = AWS_S3_Download(logger, download_file, bucket_name, s3_key)
  aws_s3.download_s3_file(aws_s3.download_approach_interactive)
