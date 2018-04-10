import base64
import json
import time
import urllib.parse
import hashlib
import hmac
import tempfile
import threading

from django.core.files import File
from django.core.files.storage import Storage

import requests.exceptions
from django.utils.functional import cached_property

extra_headers = {
    'User-Agent': 'Backathon+Python/3 <github.com/brownan/backathon>'
}

# Timeout used in HTTP calls
TIMEOUT = 5

class B2Storage(Storage):
    """Django storage backend for Backblaze B2

    B2's object store doesn't fit perfectly into Django's storage abstraction
    for our use case. B2 has three classes of transactions: class A are free,
    class B cost a bit of money, and class C cost an order of magnitude more
    per request. So this class and the calling code should access B2 in a
    pattern that minimises unnecessary requests.

    This turns out to be tricky. A naïve implementation may implement file
    metadata functions (Storage.size(), Storage.exists(), etc) as calls to
    b2_get_file_info and downloads as a call to b2_download_file_by_name,
    both class B transactions. But notice that B2 gives you quite a lot of
    information along with a b2_list_file_names call, which can return file
    metadata for up to 1000 files in bulk and therefore save on
    transaction costs despite being a class C transaction. Unfortunately,
    Django's Storage class doesn't have an equivalent call; the listdir()
    call is expected to return names, not a data structure of information on
    each file.

    To support workflows that loop over results of listdir() and get metadata
    on each one, we implement a metadata cache. Calls to get metadata on
    files that have recently been iterated over will not incur another call
    to a b2 API.

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

    @cached_property
    def bucket_id(self):
        """The bucket ID

        Some B2 API calls require the bucket ID, some require the name. Since
        the name is part of our config, we have to query for the ID
        """
        data = self._call_api("b2_list_buckets", {'accountId': self.account_id})
        for bucketinfo in data['buckets']:
            if bucketinfo['bucketName'] == self.bucket_name:
                return bucketinfo['bucketId']

        raise IOError("No such bucket name {}".format(self.bucket_name))

    @property
    def _session(self):
        # Initialize a new requests.Session for this thread if one doesn't
        # exist
        try:
            return self._local.session
        except AttributeError:
            session = requests.Session()
            session.headers.update(extra_headers)
            self._local.session = session
            return session

    @property
    def _metadata_cache(self):
        # Maps filenames to metadata dicts as returned by b2_get_file_info
        # and several other calls. This is used to cache metadata between
        # calls to Storage.listdir() and other Storage.* methods that get
        # metadata, so the file doesn't have to be downloaded if calling code
        # just wants to loop over file names and get some metadata about
        # each one.
        try:
            return self._local.metadata_cache
        except AttributeError:
            self._local.metadata_cache = {}
            return self._local.metadata_cache

    def _post_with_backoff_retry(self, *args, **kwargs):
        """Calls self._session.post with the given arguments

        Implements automatic retries and backoffs as per the B2 documentation
        """
        if "timeout" not in kwargs:
            kwargs['timeout'] = TIMEOUT

        delay = 1
        max_delay = 64
        while True:
            try:
                response = self._session.post(*args, **kwargs)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                # No response from server at all
                if max_delay < delay:
                    # Give up
                    raise
                time.sleep(delay)
                delay *= 2
            else:
                if response.status_code == 503:
                    # Service unavailable
                    if max_delay < delay:
                        # Give up
                        return response
                    time.sleep(delay)
                    delay *= 2
                elif response.status_code == 429:
                    # Too many requests
                    time.sleep(int(response.headers.get('Retry-After', 1)))
                    delay = 1
                else:
                    # Success. Or at least, not a response that we want to retry
                    return response


    def _authorize_account(self):
        """Calls b2_authorize_account to get a session authorization token

        If successful, sets the authorization_token and api_url

        If unsuccessful, raises an IOError with a description of the error
        """
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
        """Calls the given API with the given data

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

        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise IOError("Invalid json response from B2 on call to {}".format(api_name))

        if response.status_code == 401 and data['code'] == "expired_auth_token":
            # Auth token has expired. Retry after getting a new one.
            self._local.api_url = None
            self._local.authorization_token = None
            return self._call_api(api_name, data)

        if response.status_code != 200:
            raise IOError(data['message'])

        return data

    def _get_upload_url(self):
        """Sets self.upload_url and self.upload_token or raises IOError"""
        data = self._call_api("b2_get_upload_url",
                              data={'bucketId': self.bucket_id})
        self._local.upload_url = data['uploadUrl']
        self._local.upload_token = data['authorizationToken']

    def _upload_file(self, name, content):
        """Calls b2_upload_file to upload the given data to the given name

        If necessary, calls b2_get_upload_url to get a new upload url

        :param content: a django File object of some sort
        :type content: File

        :returns: the result from the upload call, a dictionary of object
        metadata

        """
        if (getattr(self._local, "upload_url", None) is None or
            getattr(self._local, "upload_token", None) is None
        ):
            self._get_upload_url()

        response = None
        response_data = None

        filename = urllib.parse.quote(name, encoding="utf-8")
        content_type = getattr(content, 'content_type', 'b2/x-auto')

        digest = hashlib.sha1()
        content.seek(0)
        for chunk in content.chunks():
            digest.update(chunk)

        # Don't use the backoff handler when uploading. For most problems
        # we just call _get_upload_url() and try again immediately
        for _ in range(5):
            response = None
            response_data = None
            
            content.seek(0)

            try:
                response = self._session.post(
                    self._local.upload_url,
                    headers = {
                        'Authorization': self._local.upload_token,
                        'X-Bz-File-Name': filename,
                        'Content-Type': content_type,
                        'Content-Length': content.size,
                        'X-Bz-Content-Sha1': digest.hexdigest(),
                    },
                    timeout=TIMEOUT,
                    data=content,
                )
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                self._get_upload_url()
                continue

            try:
                response_data = response.json()
            except ValueError():
                raise IOError("Invalid json returned from B2 API")

            if response.status_code == 401 and response_data['code'] == "expired_auth_token":
                self._get_upload_url()
                continue

            if response.status_code == 408:
                # Request timeout
                self._get_upload_url()
                continue

            if 500 <= response.status_code <= 599:
                # Any server errors
                self._get_upload_url()
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
        raise IOError("Upload failed, unknown reason")

    def _download_file(self, name):
        """Downloads a file by name

        Returns (metadata dict, file handle)

        file handle is open for reading in binary mode.

        Raises IOError if there was a problem downloading the file
        """
        f = tempfile.SpooledTemporaryFile(
            suffix=".b2tmp"
        )

        if (getattr(self._local, "download_url", None) is None or
            getattr(self._local, "authorization_token", None) is None
        ):
            self._authorize_account()

        digest = hashlib.sha1()

        response = self._session.get(
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

        with response:
            if response.status_code != 200:
                try:
                    resp_json = response.json()
                except ValueError:
                    response.raise_for_status()
                    raise IOError("Non-200 status code returned for download "
                                  "request")
                raise IOError(resp_json['message'])

            for chunk in response.iter_content(chunk_size=B2File.DEFAULT_CHUNK_SIZE):
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

    def _save(self, name, content):
        """Saves a file under the given name"""
        metadata = self._upload_file(name, content)
        return metadata['fileName']

    def _open(self, name, mode="rb"):
        """Opens a file for reading. Opening a file for writing is not
        currently supported

        """
        if "w" in mode:
            raise NotImplementedError("Opening files for writing is not "
                                      "supported")
        if "b" not in mode:
            raise NotImplementedError("Automatic encoding is not supported. "
                                      "Open file in binary mode")

        return B2File(name, self, data=self._metadata_cache.get(name, None))

    def _get_files_by_prefix(self, prefix):
        """Helper method for listdir(). See listdir() docstring"""
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

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
            self._metadata_cache.update(
                (d['fileName'], d) for d in data['files']
                if d['action'] == 'upload'
            )
            yield from (d['fileName'] for d in data['files'] if d['action'] == 'upload')
            start_filename = data['nextFileName']
            if start_filename is None:
                break
        self._metadata_cache.clear()

    ############################
    # Public Storage API methods
    ############################

    def get_available_name(self, name, max_length=None):
        """Overwrite existing files with the same name"""
        return name

    def get_valid_name(self, name):
        """No special filename adjustments are made

        Instead we let the B2 api return an error on invalid filenames.
        Because of our policy of overwriting existing files, the choice was
        made to not adjust names automatically.
        """
        return name

    def delete(self, name):
        """Deletes the given file

        In B2 this calls the b2_hide_file API. If you want to recover the space
        taken by this file, make sure you have your bucket lifecycle policy
        set to delete hidden files"""
        self._call_api(
            "b2_hide_file",
            {
                'bucketId': self.bucket_id,
                'fileName': name,
            }
        )


    def exists(self, name):
        """exists() call is not currently implemented

        There's not an efficient way to check this. We could download the
        file, incurring a class B transaction and the bandwidth costs,
        or we could list all files in the bucket incurring a class C
        transaction. Callers are advised to listdir() and use the
        resulting list to check what they need.
        """
        raise NotImplementedError("Not currently implemented")

    def listdir(self, path):
        """List files with a given path prefix in the bucket

        This method returns an iterator over filenames with the given prefix.
        Note that this means it's effectively a recursive directory listing
        as subdirectory contents are also listed.

        Because B2 is an object store, the returned list of directories is
        always empty. Only files are returned.

        :returns: ([], filename_iterator)

        For very large buckets, this method efficiently batches calls to the
        API to return up to 1000 files per call. No more than 1000 entries
        are held in memory at a time.

        This method also caches the metadata for each file. Callers that
        iterate over entries from this call may use the metadata methods such
        as B2Storage.size(), B2Storage.get_modified_time(), or access
        metadata properties on B2File objects returned from B2Storage.open()
        without incurring additional B2 service API calls.
        Any other usage pattern may require additional calls to the B2 API.

        """
        return [], self._get_files_by_prefix(path)

    def size(self, name):
        return self._open(name).size

    def url(self, name):
        # TODO
        raise NotImplementedError("Not currently implemented")

    def get_accessed_time(self, name):
        raise NotImplementedError("Access time is not implemented for B2")

    def get_created_time(self, name):
        return self._open(name).modified_time

    def get_modified_time(self, name):
        return self._open(name).modified_time

class B2File(File):
    """An object in B2

    Such objects hold metadata, and the contents are downloaded on demand
    when requested.
    """
    def __init__(self, name, storage, data=None):
        """Initiate a new B2File, representing an object that exists in B2

        data is a dictionary as returned by b2_get_file_info. It
        should have (at least) these keys:

        * fileId
        * fileName
        * contentSha1
        * contentLength
        * contentType
        * fileInfo
        * uploadTimestamp

        :type name: str
        :type storage: B2Storage
        :type data: dict

        """
        self.name = name
        self.storage = storage
        self.mode = "rb"

        self.data = data
        self._file = None

    @property
    def size(self):
        if self.data is None:
            self.load()
        return self.data['contentLength']

    @property
    def sha1(self):
        if self.data is None:
            self.load()
        return self.data['contentSha1']

    @property
    def content_type(self):
        if self.data is None:
            self.load()
        return self.data['contentType']

    @property
    def modified_time(self):
        if self.data is None:
            self.load()
        return self.data['uploadTimestamp']

    @property
    def file(self):
        if self._file is None:
            self.load()
        return self._file

    def load(self):
        self.data, self._file = self.storage._download_file(self.name)
