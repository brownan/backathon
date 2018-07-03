"""
This is the interface to the Backblaze B2 API

The methods in this module are written with efficiency in mind for our
workload. Since some API calls cost real money, we have the goal of
minimizing costs for common operations. Also since we may potentially have
millions of objects stored, we also must be conscious of memory efficiency.

The original plan was to make a Django Storage compatible class, but that
quickly became difficult to do efficiently because of differences in the B2
API and the Storage API.

For example, there's no easy way to efficiently iterate over a huge
number of objects with the Django storage API, since the listdir() call
returns a 2-tuple of lists, the first is a list of directories and the second a
list of files. Further, the B2 API gives a lot of metadata with the list
call, but the Django Storage.listdir() call only returns names. A naïve
implementation would require a B2 call to b2_get_file_info costing a class B
transaction for each file. None of the problems are impossible, but would
require complicated caching and would have fast and slow access patterns.

Seeing as how my goal is not to make a generic Django B2 Storage backend,
I chose to optimize the B2 interface for my needs and to keep it simple.


Cost reference (accurate as of July 2 2018)

Class A Transactions (Free)
b2_delete_key
b2_delete_bucket
b2_delete_file_version
b2_hide_file
b2_get_upload_url
b2_upload_file
b2_start_large_file
b2_get_upload_part_url
b2_upload_part
b2_cancel_large_file
b2_finish_large_file

Class B Transactions ($0.004 per 10,000)
b2_download_file_by_id
b2_download_file_by_name
b2_get_file_info

Class C Transactions ($0.004 per 1,000)
b2_authorize_account
b2_create_key
b2_list_keys
b2_create_bucket
b2_list_buckets
b2_list_file_names
b2_list_file_versions
b2_update_bucket
b2_list_parts
b2_list_unfinished_large_files
b2_get_download_authorization

"""
import base64
import io
import os
import time
import urllib.parse
import hashlib
import hmac
import tempfile
import threading
from logging import getLogger

import requests.exceptions
from django.utils.functional import cached_property

logger = getLogger("backathon.b2")

extra_headers = {
    'User-Agent': 'Backathon/Python3 <github.com/brownan/backathon>'
}

# Timeout used in HTTP calls
TIMEOUT = 5

class B2ResponseError(IOError):
    def __init__(self, data):
        super().__init__(data['message'])
        self.data = data

class B2Bucket:
    """Represents a B2 Bucket, a container for objects

    This object should be thread safe, as it keeps a thread-local
    requests.Session object for API calls, as well as thread-local
    authorization tokens.

    """
    def __init__(self,
                 account_id,
                 application_key,
                 bucket_name,
                 ):
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_name = bucket_name

        # Thread local variables hold the requests Session object, as well as
        # various authorization tokens acquired from B2
        self._local = threading.local()

    @property
    def session(self):
        # Initialize a new requests.Session for this thread if one doesn't
        # exist
        try:
            return self._local.session
        except AttributeError:
            logger.debug("Initializing session for thread id {}".format(
                threading.get_ident()
            ))
            session = requests.Session()
            session.headers.update(extra_headers)
            self._local.session = session
            return session

    def _post_with_backoff_retry(self, *args, **kwargs):
        """Calls self.session.post with the given arguments

        Implements automatic retries and backoffs as per the B2 documentation
        """
        if "timeout" not in kwargs:
            kwargs['timeout'] = TIMEOUT

        delay = 1
        max_delay = 64
        while True:
            try:
                response = self.session.post(*args, **kwargs)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                # No response from server at all
                if max_delay < delay:
                    # Give up
                    logger.info("Timeout in B2 call. Giving up")
                    raise
                logger.debug("Timeout in B2 call, retrying in {}s".format(
                    delay))
                time.sleep(delay)
                delay *= 2
            else:
                if response.status_code == 503:
                    # Service unavailable
                    if max_delay < delay:
                        # Give up
                        logger.info("B2 service unavailable. Giving up")
                        return response
                    logger.debug("B2 service unavailable, retrying in "
                                 "{}s".format(delay))
                    time.sleep(delay)
                    delay *= 2
                elif response.status_code == 429:
                    # Too many requests
                    delay = int(response.headers.get('Retry-After', 1))
                    logger.debug("B2 returned 429 Too Many Requests. "
                                  "Retrying in {}s".format(delay))
                    time.sleep(delay)
                    delay = 1
                else:
                    # Success. Or at least, not a response that we want to retry
                    return response

    def _authorize_account(self):
        """Calls b2_authorize_account to get a session authorization token

        If successful, sets the authorization_token and api_url

        If unsuccessful, raises an IOError with a description of the error

        This costs one class C transaction. This generally needs to be called
        once per thread at the start of the session, but extremely long
        sessions may need to refresh the authorization token.
        """
        logger.debug("Acquiring authorization token for thread id {}".format(
            threading.get_ident()
        ))
        response = self._post_with_backoff_retry(
            "https://api.backblazeb2.com/b2api/v1/b2_authorize_account",
            headers={
                'Authorization': 'Basic {}'.format(
                    base64.b64encode("{}:{}".format(
                        self.account_id,
                        self.application_key
                    ).encode("ASCII")).decode("ASCII")
                ),
            },
            json={},
        )

        try:
            data = response.json()
        except ValueError:
            # Invalid or no JSON returned from response
            response.raise_for_status()
            raise IOError("Invalid json response from B2")

        if response.status_code != 200:
            raise IOError("{}: {}".format(response.status_code,
                                          data['message']))

        self._local.authorization_token = data['authorizationToken']
        self._local.api_url = data['apiUrl']
        self._local.download_url = data['downloadUrl']

    def _call_api(self, api_name, data):
        """Calls the given API with the given json data

        If the account hasn't been authorized yet, calls b2_authorize_account
        first to obtain the authorization token

        If successful, returns the response json object

        If unsuccessful, raises an IOError with a description of the error
        """
        api_url = getattr(self._local, 'api_url', None)
        authorization_token = getattr(self._local, 'authorization_token', None)
        if api_url is None or authorization_token is None:
            self._authorize_account()

        response = self._post_with_backoff_retry(
            "{}/b2api/v1/{}".format(self._local.api_url, api_name),
            headers = {
                'Authorization': self._local.authorization_token,
            },
            json=data,
        )
        logger.debug("{} {} {:.2f}s".format(
            api_name,
            response.status_code,
            response.elapsed.total_seconds(),
        ))

        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise IOError("Invalid json response from B2 on call to {}".format(api_name))

        if response.status_code == 401 and data['code'] == "expired_auth_token":
            # Auth token has expired. Retry after getting a new one.
            self._local.api_url = None
            self._local.authorization_token = None
            logger.info("Auth token expired")
            return self._call_api(api_name, data)

        if response.status_code != 200:
            raise B2ResponseError(data)

        return data

    @cached_property
    def bucket_id(self):
        """The bucket ID

        Some B2 API calls require the bucket ID, some require the name. Since
        the name is part of our config, we have to query for the ID.

        This costs one class C transaction, and the result is cached for the
        lifetime of this B2Bucket instance.
        """
        data = self._call_api("b2_list_buckets", {'accountId': self.account_id})
        for bucketinfo in data['buckets']:
            if bucketinfo['bucketName'] == self.bucket_name:
                return bucketinfo['bucketId']

        raise IOError("No such bucket name {}".format(self.bucket_name))

    def _get_upload_url(self):
        """Sets the upload_url and upload_token or raises IOError

        This costs one class A transaction
        """
        logger.debug("Getting a new upload url")
        data = self._call_api("b2_get_upload_url",
                              data={'bucketId': self.bucket_id})
        self._local.upload_url = data['uploadUrl']
        self._local.upload_token = data['authorizationToken']

    def upload_file(self, name, content):
        """Calls b2_upload_file to upload the given data to the given name

        If necessary, calls b2_get_upload_url to get a new upload url

        :param name: The name of the object to upload.

        :param content: A file-like object open for reading. Make sure the
            file is opened in binary mode

        :returns: the result from the upload call, a dictionary of object
            metadata

        This costs one class A transaction for the upload, and possibly a
        second for the call to b2_get_upload_url

        """
        logger.info("Uploading {!r}".format(name))

        response = None
        response_data = None
        exc_str = None

        filename = urllib.parse.quote(name, encoding="utf-8")

        content.seek(0, os.SEEK_END)
        filesize = content.tell()

        content.seek(0)
        digest = hashlib.sha1()
        while True:
            chunk = content.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break
            digest.update(chunk)

        headers = {
            'X-Bz-File-Name': filename,
            'Content-Type': "b2/x-auto",
            'Content-Length': str(filesize),
            'X-Bz-Content-Sha1': digest.hexdigest(),
        }

        # We don't use the usual backoff handler when uploading. As per the B2
        # documentation, for most problems we can just get a new upload URL
        # with b2_get_upload_url and try again immediately
        for _ in range(5):
            if (getattr(self._local, "upload_url", None) is None or
                getattr(self._local, "upload_token", None) is None
            ):
                self._get_upload_url()

            headers['Authorization'] = self._local.upload_token

            response = None
            response_data = None
            exc_str = None
            
            content.seek(0)

            try:
                response = self.session.post(
                    self._local.upload_url,
                    headers = headers,
                    timeout=TIMEOUT,
                    data=content,
                )
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                logger.info("Error when uploading ({})".format(e))
                exc_str = str(e)
                del self._local.upload_url
                continue

            logger.debug("b2_upload_file {} {:.2f}s".format(
                response.status_code,
                response.elapsed.total_seconds(),
            ))

            try:
                response_data = response.json()
            except ValueError():
                raise IOError("Invalid json returned from B2 API")

            if response.status_code == 401 and response_data['code'] == "expired_auth_token":
                logger.info("Expired auth token when uploading")
                del self._local.upload_url
                continue

            if response.status_code == 408:
                # Request timeout
                logger.info("Request timeout when uploading")
                del self._local.upload_url
                continue

            if 500 <= response.status_code <= 599:
                # Any server errors
                logger.info("Server error when uploading")
                del self._local.upload_url
                continue

            # Any other errors indicate a permanent problem with the request
            if response.status_code != 200:
                raise IOError(response_data['message'])

            return response_data

        # The loop exited, so all 5 tries failed. See if we can raise an
        # appropriate error from the last try
        if response_data is not None:
            raise IOError(response_data['message'])
        if response is not None:
            response.raise_for_status()
        # This path could be hit if the last failure was due to a connection
        # error or timeout
        if exc_str:
            raise IOError(exc_str)
        raise IOError("Upload failed, unknown reason")

    def download_file(self, name):
        """Downloads a file by name

        Returns (metadata dict, file handle)

        file handle is open for reading in binary mode.

        Raises IOError if there was a problem downloading the file

        This costs one class B transaction
        """
        logger.debug("Downloading {}".format(name))
        f = tempfile.SpooledTemporaryFile(
            suffix=".b2tmp",
            max_size=2**21,
        )

        if (getattr(self._local, "download_url", None) is None or
            getattr(self._local, "authorization_token", None) is None
        ):
            self._authorize_account()

        digest = hashlib.sha1()

        response = self.session.get(
            "{}/file/{}/{}".format(
                self._local.download_url,
                self.bucket_name,
                name
            ),
            timeout=TIMEOUT,
            headers={
                'Authorization': self._local.authorization_token,
            },
            stream=True,
        )

        logger.debug("b2_download_file_by_name {} {:.2f}s".format(
            response.status_code,
            response.elapsed.total_seconds(),
        ))

        with response:
            if response.status_code != 200:
                try:
                    resp_json = response.json()
                except ValueError:
                    response.raise_for_status()
                    raise IOError("Non-200 status code returned for download "
                                  "request")
                raise IOError(resp_json['message'])

            for chunk in response.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE):
                digest.update(chunk)
                f.write(chunk)

        if not hmac.compare_digest(
                digest.hexdigest(),
                response.headers['X-Bz-Content-Sha1'],
        ):
            f.close()
            raise IOError("Corrupt download: Sha1 doesn't match")

        data = {
            'fileId': response.headers['X-Bz-File-Id'],
            'fileName': response.headers['X-Bz-File-Name'],
            'contentSha1': response.headers['X-Bz-Content-Sha1'],
            'contentLength': response.headers['Content-Length'],
            'contentType': response.headers['Content-Type'],
            'uploadTimestamp': response.headers['X-Bz-Upload-Timestamp'],
            'fileInfo': {},
        }

        for h in response.headers:
            if h.startswith("X-Bz-Info-"):
                data['fileInfo'][h[10:]] = response.headers[h]

        f.seek(0)
        return data, f

    def get_files_by_prefix(self, prefix):
        """Calls b2_list_file_names to get a list of files in the bucket

        :param prefix: A file name prefix. This is not a directory path,
        but a string prefix. All objects in the bucket with the given prefix
        will be returned.

        :returns: An iterator over metadata dictionaries

        Note: we fetch 1000 items at a time from the underlying API, so this
        method is memory efficient over very large result sets.

        This costs one class C transaction per 1000 objects returned (rounded
        up)
        """

        start_filename = None

        while True:
            data = self._call_api(
                "b2_list_file_names",
                {
                    'bucketId': self.bucket_id,
                    # 1000 is the maximum number of items that can be
                    # returned in a single class C transaction. The API can
                    # return more but it will charge multiple transactions.
                    # This may be worth it if we discover it gives us
                    # performance gains.
                    'maxFileCount': 1000,
                    'prefix': prefix,
                    'startFileName': start_filename,
                }
            )
            yield from data['files']
            start_filename = data['nextFileName']
            if start_filename is None:
                break

    def delete(self, name):
        """Deletes the given file

        In B2 this calls the b2_hide_file API. If you want to recover the space
        taken by this file, make sure you have your bucket lifecycle policy
        set to delete hidden files

        This costs one class A transaction

        This call ignores errors for the file not existing or the file being
        already hidden
        """
        try:
            self._call_api(
                "b2_hide_file",
                {
                    'bucketId': self.bucket_id,
                    'fileName': name,
                }
            )
        except B2ResponseError as e:
            if e.data['code'] in ('no_such_file', 'already_hidden'):
                pass
            else:
                raise

