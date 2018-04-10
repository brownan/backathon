import base64
import json
import time
import urllib.parse
import weakref
import hashlib
import hmac
import tempfile

from django.core.files import File
from django.core.files.storage import Storage
from django.utils.functional import cached_property

import requests.exceptions


extra_headers = {
    'User-Agent': 'Backathon+Python/3'
}

# Timeout used in HTTP calls
TIMEOUT = 5

class B2Storage(Storage):
    """Django storage backend for Backblaze B2

    A new instance of this class should be created for each upload thread to
    parallelize uploads. Instances should not be shared among threads.

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
    each file. So this class must either do some clever caching (more memory
    usage) or we must create our own storage abstraction layer on top of
    Django's that provides the calls we need.

    To keep with the goal of having Backathon support any object store with a
    Django Storage implementation, we choose the former option and tune the
    caching done by this class to match the usage patterns of the access
    patterns performed by Backathon.

    In particular, a listdir call will return an iterator over all files,
    and internally it will perform a b2_list_file_names call. The API call
    returns 1000 entries per transaction, and the metadata entries are saved
    while just the names are returned. Subsequent calls to B2Storage.size()
    and other calls that involve a name parameter will pull from the cached
    metadata. So as long as the access pattern involves looping over
    filenames and performing some operations on them, no other calls to
    b2_get_file_info will be necessary.

    Other situations involve some custom APIs. B2 gives us the object SHA1
    hash as metadata, and that could be very useful to know without having to
    download the file. Since this isn't a feature provided by all storage
    providers, this necessarily requires a custom Storage api call,
    and callers have to have a fall back case if that call doesn't exist.
    """
    def __init__(self,
                 account_id,
                 application_key,
                 bucket_id,
                 ):
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id

        self._session = requests.Session()
        self._session.headers.update(extra_headers)

        # These parameters are acquired by calls to the API and cached
        self.authorization_token = None
        self.api_url = None
        self.download_url = None
        self.upload_url = None
        self.upload_token = None

        # Keeps track of File objects this class has handed out
        self._file_entries = weakref.WeakValueDictionary()

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

        If successful, sets self.authorization_token and self.api_url

        If unsuccessful, raises an IOError with a description of the error
        """
        response = self._post_with_backoff_retry(
            "https://api.backblazeb2.com/b2api/v1/b2_authorize_account",
            headers={
                'Authorization': 'Basic {}'.format(
                    base64.b64encode("{}:{}".format(
                        self.account_id,
                        self.application_key
                    ))
                ),
            },
        )

        try:
            data = response.json()
        except ValueError:
            # Invalid or no JSON returned from response
            response.raise_for_status()
            raise IOError("Invalid json response from B2")

        if response.status_code != 200:
            raise IOError(data['message'])

        self.authorization_token = data['authorizationToken']
        self.api_url = data['apiUrl']
        self.download_url = data['downloadUrl']

    def _call_api(self, api_name, data):
        """Calls the given API with the given data

        If the account hasn't been authorized yet, calls b2_authorize_account
        first to obtain the authorization token

        If successful, returns the response json object

        If unsuccessful, raises an IOError with a description of the error
        """
        if self.api_url is None or self.authorization_token is None:
            self._authorize_account()

        response = self._post_with_backoff_retry(
            "{}/b2api/v1/{}".format(self.api_url, api_name),
            headers = {
                'Authorization': self.authorization_token,
            },
            data=json.dumps(data),
        )

        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise IOError("Invalid json response from B2 on call to {}".format(api_name))

        if response.status_code == 401 and data['code'] == "expired_auth_token":
            # Auth token has expired. Retry after getting a new one.
            self.api_url = None
            self.authorization_token = None
            return self._call_api(api_name, data)

        if response.status_code != 200:
            raise IOError(data['message'])

        return data

    def _get_upload_url(self):
        """Sets self.upload_url and self.upload_token or raises IOError"""
        data = self._call_api("b2_get_upload_url",
                              data={'bucketId': self.bucket_id})
        self.upload_url = data['uploadUrl']
        self.upload_token = data['authorizationToken']

    def _upload_file(self, name, file):
        """Calls b2_upload_file to upload the given data to the given name

        If necessary, calls b2_get_upload_url to get a new upload url

        :param file: a django File object of some sort
        :type file: File

        :returns: the result from the upload call, a dictionary of object
        metadata

        """
        if not self.upload_url or not self.upload_token:
            self._get_upload_url()

        response = None
        response_data = None

        filename = urllib.parse.quote(name, encoding="utf-8")
        content_type = getattr(file, 'content_type', 'application/octet-stream')

        digest = hashlib.sha1()
        file.seek(0)
        for chunk in file.chunks():
            digest.update(chunk)

        # Don't use the backoff handler when uploading. For most problems
        # we just call _get_upload_url() and try again immediately
        for _ in range(5):
            response = None
            response_data = None
            
            file.seek(0)

            try:
                response = self._session.post(
                    self.upload_url,
                    headers = {
                        'Authorization': self.upload_token,
                        'X-Bz-File-Name': filename,
                        'Content-Type': content_type,
                        'Content-Length': file.size,
                        'X-Bz-Content-Sha1': digest.hexdigest(),
                    },
                    timeout=TIMEOUT,
                    data=file,
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

    def _save(self, name, content):
        """Saves a file under the given name"""
        pass # TODO

    def _open(self, name, mode):
        """Opens a file for reading. Opening a file for writing is not
        currently supported"""
        if "w" in mode:
            raise NotImplementedError("Opening files for writing is not "
                                      "supported")
        if "b" not in mode:
            raise NotImplementedError("Automatic encoding is not supported. "
                                      "Open file in binary mode")

        pass # TODO

class B2File(File):
    """An object in B2

    Such objects hold metadata, and the contents are downloaded on demand
    when requested.
    """
    def __init__(self, name, storage, data):
        """Initiate a new B2File, representing an object that exists in B2

        data is a dictionary as returned by b2_get_file_info. It
        should have (at least) these keys:

        * fileId
        * fileName
        * contentSha1
        * contentLength
        * contentType
        * fileInfo
        * action
        * uploadTimestamp

        :type name: str
        :type storage: B2Storage
        :type data: dict

        """
        self.name = name
        self.storage = storage
        self.mode = "rb"

        self.data = data

    @property
    def size(self):
        return self.data['contentLength']

    @property
    def sha1(self):
        return self.data['contentSha1']

    @cached_property
    def file(self):
        f = tempfile.SpooledTemporaryFile(
            suffix=".b2tmp"
        )

        if not self.storage.download_url or not self.storage.authorization_token:
            # Not sure how it would have created this object if it wasn't
            # authorized, but some things may invalidate the authorization
            # tokens
            self.storage._authorize_account()

        response = self.storage._session.get(
            "{}/file/{}/{}".format(
                self.storage.download_url,
                self.storage.bucket_id,
                self.name
            ),
            timeout=TIMEOUT,
            headers={
                'Authorization', self.storage.authorization_token,
            },
            stream=True,
        )

        digest = hashlib.sha1()

        with response:
            for chunk in response.iter_content(chunk_size=self.DEFAULT_CHUNK_SIZE):
                digest.update(chunk)
                f.write(chunk)

        if not hmac.compare_digest(
                digest.hexdigest(),
                response.headers['X-Bz-Content-Sha1'],
        ):
            f.close()
            raise IOError("Corrupt download: Sha1 doesn't match")

        f.seek(0)
        return f

