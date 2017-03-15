from tornado.concurrent import return_future
from gcloud import storage
from collections import defaultdict

import thumbor.loaders.http_loader as http_loader

buckets = defaultdict(dict)

@return_future
def load(context, path, callback):
    if path.startswith('http'): #_use_http_loader(context, url):
        logger.debug("[LOADER] load with http_loader")
        http_loader.load_sync(context, path, callback, normalize_url_func=http_loader._normalize_url)
        return

    bucket_id  = context.config.get("CLOUD_STORAGE_BUCKET_ID")
    project_id = context.config.get("CLOUD_STORAGE_PROJECT_ID")
    bucket = buckets[project_id].get(bucket_id, None)

    logger.debug("[LOADER] loading from bucket")

    if bucket is None:
        client = storage.Client(project_id)
        bucket = client.get_bucket(bucket_id)
        buckets[project_id][bucket_id] = bucket

    blob = bucket.get_blob(path)
    if blob:
        callback(blob.download_as_string())
    else:
        callback(blob)
