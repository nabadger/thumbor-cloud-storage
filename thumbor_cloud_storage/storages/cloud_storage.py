#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import pytz
import re
import os
import hashlib

from tornado.concurrent import return_future
from thumbor.result_storages import BaseStorage
from thumbor.engines import BaseEngine
from thumbor.utils import logger

from gcloud import storage


class Storage(BaseStorage):

    PATH_FORMAT_VERSION = 'v2'
    bucket = None

    no_http = re.compile(r"^http.?(\%3A|:)//", re.IGNORECASE)

    def __init__(self, context):
        self.context = context
        self.bucket = self._get_bucket()

    @property
    def is_auto_webp(self):
        return self.context.config.AUTO_WEBP \
            and self.context.request.accepts_webp

    def put(self, path, bytes):
        start = datetime.datetime.now()
        normalized_path = self._normalize_path(path)
        content_type = 'text/plain'
        if bytes:
            try:
                mime = BaseEngine.get_mimetype(bytes)
                content_type = mime
            except:
                logger.error("[GoogleCloudStorage] Couldn't determine mimetype for %s"
                              % path)

        blob = self._get_bucket().blob(normalized_path)
        blob.upload_from_string(bytes, content_type=content_type)

        finish = datetime.datetime.now()
        self.context.metrics.timing('gcs.put.{0}'.format(normalized_path),
                                    (finish - start).total_seconds()
                                    * 1000)

    def put_crypto(self, path):
        '''
    :returns: Nothing. This method is expected to be asynchronous.
    :rtype: None
    '''

        return

    def put_detector_data(self, path, data):
        '''
    :returns: Nothing. This method is expected to be asynchronous.
    :rtype: None
    '''

        return

    @return_future
    def get_crypto(self, path, callback):
        callback(None)

    @return_future
    def get_detector_data(self, path, callback):
        callback(None)

    def get(self, path):
        logger.debug('[GoogleCloudStorage] get path %s' % path)
        start = datetime.datetime.now()
        normalized_path = self._normalize_path(path)
        blob = self._get_bucket().get_blob(normalized_path)

        data = None
        if blob and not self._is_expired(blob):
            data = blob.download_as_string()
        else:
            logger.debug('[GoogleCloudStorage] blob not found or expired %s'
                          % normalized_path)

        finish = datetime.datetime.now()
        self.context.metrics.timing('gcs.fetch.{0}'.format(normalized_path),
                                    (finish - start).total_seconds()
                                    * 1000)
        return data

    def exists(self, path):
        normalized_path = self._normalize_path(path)
        blob = self._get_bucket().get_blob(normalized_path)

        if not blob or self._is_expired(blob):
            return False
        return True

    def remove(self, path):
        raise NotImplementedError()

    def _is_expired(self, blob):
        expire_in_seconds = \
            self.context.config.get('STORAGE_EXPIRATION_SECONDS', None)

        if expire_in_seconds is None or expire_in_seconds == 0:
            return False

        timediff = datetime.datetime.now(pytz.utc) - blob.updated
        return timediff.seconds > expire_in_seconds

    def _normalize_path(self, path):
        path_segments = \
            [self.context.config.get('CLOUD_STORAGE_ROOT_PATH',
             'thumbor/').rstrip('/'), Storage.PATH_FORMAT_VERSION]

    # path_segments.extend([self.partition(path), path.lstrip('/'), ])

        path_segments.extend([path.lstrip('/')])

        normalized_path = os.path.join(*path_segments).replace('http://'
                , '')
        logger.debug('[normalize_path] path=%s, normalized_path=%s'
                     % (path, normalized_path))
        return normalized_path

    def _get_bucket(self):
        parent = Storage
        if not parent.bucket:
            bucket_id = \
                self.context.config.get('CLOUD_STORAGE_BUCKET_ID')
            project_id = \
                self.context.config.get('CLOUD_STORAGE_PROJECT_ID')

            logger.debug('[GoogleCloudStorage] getting bucket %s'
                         % bucket_id)
            client = storage.Client(project_id)
            parent.bucket = client.get_bucket(bucket_id)

        return parent.bucket
