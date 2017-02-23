#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/thumbor/thumbor/wiki

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 globo.com thumbor@googlegroups.com

import os
from os.path import join
from shutil import move
from json import dumps, loads
from datetime import datetime
from os.path import exists, dirname, getmtime, splitext
import hashlib
from uuid import uuid4
from gcloud import storage

#import thumbor.storages as storages
from thumbor.storages import BaseStorage
from thumbor.utils import logger
from tornado.concurrent import return_future

# Storage
class Storage(BaseStorage):
    PATH_FORMAT_VERSION = 'v2'
    bucket = None

    def __init__(self, context, shared_client=True):
        BaseStorage.__init__(self, context)
        self.shared_client = shared_client
        self.bucket = self.get_bucket()

    def put(self, path, bytes):
        file_abspath = self.normalize_path(path)
        logger.debug("[STORAGE] putting at %s" % file_abspath)
        bucket = self.get_bucket()

        blob = bucket.blob(file_abspath)
        blob.upload_from_string(bytes)

        max_age = self.context.config.CLOUD_STORAGE_MAX_AGE
        blob.cache_control = "public,max-age=%s" % max_age

        if bytes:
            try:
                mime = BaseEngine.get_mimetype(bytes)
                blob.content_type = mime
            except:
                logger.debug("[STORAGE] Couldn't determine mimetype")

        blob.patch()

    def put_crypto(self, path):
	pass

    @return_future
    def exists(self, path, callback, path_on_filesystem=None):
        file_abspath = self.normalize_path(path)
        logger.debug("[STORAGE] checking existance of %s" % file_abspath)

        bucket = self.get_bucket()
        blob = bucket.get_blob(file_abspath)
        if not blob:
            logger.debug("[STORAGE] no blob: %s" % file_abspath)
            callback(False)
        else:
            logger.debug("[STORAGE] check blob exists: %s" % file_abspath)
            callback(blob.exists())

    @return_future
    def get(self, path, callback, path_on_filesystem=None):
        file_abspath = self.normalize_path(path)
        logger.debug("[STORAGE] getting from %s" % file_abspath)

        bucket = self.get_bucket()
        blob = bucket.get_blob(file_abspath)
        if not blob:
            logger.debug("[STORAGE] [get] no blob: %s" % file_abspath)
            callback(None)
        else:
            logger.debug("[STORAGE] [get] check blob exists: %s" % file_abspath)
            callback(blob.download_as_string())

    def remove(self, path):
        logger.debug("[STORAGE] remove not implemented")
        pass

    # private

    def get_bucket(self):
        parent = self
        if self.shared_client:
            parent = Storage
        if not parent.bucket:
            bucket_id  = self.context.config.get("CLOUD_STORAGE_BUCKET_ID")
            project_id = self.context.config.get("CLOUD_STORAGE_PROJECT_ID")
            client = storage.Client(project_id)
            parent.bucket = client.get_bucket(bucket_id)
        return parent.bucket

    def normalize_path(self, path):
        path_segments = [self.context.config.get('CLOUD_STORAGE_ROOT_PATH', 'thumbor/').rstrip('/'), Storage.PATH_FORMAT_VERSION, ]
        #path_segments.extend([self.partition(path), path.lstrip('/'), ])
        path_segments.extend([path.lstrip('/'), ])

        normalized_path = join(*path_segments).replace('http://', '')
        return normalized_path