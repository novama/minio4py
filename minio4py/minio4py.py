import io
import logging
import os
from datetime import timedelta
from pathlib import PureWindowsPath
from posixpath import join as posixpath_join
from posixpath import normpath as posixpath_norm
from typing import Any, Dict, Generator, List, Iterable, Optional, Union

from minio import Minio
from minio.commonconfig import CopySource
from minio.datatypes import Object, Tags
from minio.deleteobjects import DeleteObject
from minio.error import S3Error
from minio.objectlockconfig import ObjectLockConfig


# Minio4Py - MinIO Client version 1.1
# https://github.com/novama/minio4py/blob/main/minio4py/minio4py.py
# For MinIO API reference:
# https://min.io/docs/minio/linux/developers/python/API.html


def minio_path_norm(object_path: str) -> str:
    """
    Normalize MinIO object path eliminating double slashes.

    :param object_path: The object name path
    :return: Object name path normalized
    """
    return posixpath_norm(PureWindowsPath(object_path).as_posix())


def minio_path_join(a: str, *paths: str) -> str:
    """Join two or more object path name components, inserting '/' as needed.
       An empty last part will result in a path that ends with a separator.

    :param a: Initial part of the object name path
    :param paths: Sequential parts of the object name path to join
    :return: Name path of the object combined
    """
    result: str = posixpath_join(a, *paths)
    return result


class Minio4Py:
    """
    A class to wrap MinIO functions for interacting with a MinIO server.
    """

    def __init__(
            self, host: str, access_key: str, secret_key: str, secure: bool = False,
            default_presigned_urls_expiration_time: timedelta = timedelta(hours=1)
    ):
        """
        Initialize the Minio4Py with the given configuration.

        :param host: MinIO server host
        :param access_key: MinIO access key
        :param secret_key: MinIO secret key
        :param secure: Use secure connection (default is False)
        :param default_presigned_urls_expiration_time: Default expiration time for presigned URLs
        """
        self.logger = logging.getLogger(__name__)

        if not host:
            raise ValueError("MinIO host cannot be None or an empty string")
        if not access_key:
            self.logger.warning("Warning: access_key defined to connect MinIO is None or an empty string")
        if not secret_key:
            self.logger.warning("Warning: secret_key defined to connect MinIO is None or an empty string")

        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.default_presigned_expiration = default_presigned_urls_expiration_time

        # Connect to MinIO server
        self.client = self.connect_to_minio()

    def connect_to_minio(self) -> Minio:
        """
        Connect to MinIO server and return the client object.

        :return: Minio client object
        """
        try:
            client = Minio(
                self.host,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            self.logger.info(f"Successfully connected to MinIO host: {self.host}")
            return client
        except Exception as e:
            self.logger.error(f"Failed to connect to MinIO host: {self.host}. Error: {e}")
            raise

    # Bucket Operations

    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a new bucket in MinIO.

        :param bucket_name: Name of the bucket to create
        """
        try:
            self.client.make_bucket(bucket_name)
            self.logger.info(f"Bucket '{bucket_name}' created successfully.")
        except S3Error:
            self.logger.warning(f"Bucket '{bucket_name}' already exists.")
        except Exception as e:
            self.logger.error(f"Error creating bucket '{bucket_name}': {e}")
            raise

    def list_buckets(self) -> None:
        """
        List all buckets in MinIO.
        """
        try:
            buckets = self.client.list_buckets()
            for bucket in buckets:
                self.logger.debug(f"Bucket: {bucket.name}, Created on: {bucket.creation_date}")
        except Exception as e:
            self.logger.error(f"Error listing buckets: {e}")
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists in MinIO.

        :param bucket_name: Name of the bucket to check
        :return: True if the bucket exists, False otherwise
        """
        try:
            exists = self.client.bucket_exists(bucket_name)
            if exists:
                self.logger.info(f"Bucket '{bucket_name}' exists.")
            else:
                self.logger.info(f"Bucket '{bucket_name}' does not exist.")
            return exists
        except S3Error as e:
            self.logger.error(f"S3Error checking if bucket '{bucket_name}' exists: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error checking if bucket '{bucket_name}' exists: {e}")
            raise

    def remove_bucket(self, bucket_name: str) -> None:
        """
        Remove a bucket from MinIO.

        :param bucket_name: Name of the bucket to remove
        """
        try:
            self.client.remove_bucket(bucket_name)
            self.logger.warning(f"Bucket '{bucket_name}' removed successfully.")
        except S3Error as e:
            self.logger.error(f"S3Error removing bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error removing bucket '{bucket_name}': {e}")
            raise

    def list_objects(self, bucket_name: str, prefix: Optional[str] = None, recursive: bool = False) -> Generator:
        """
        List objects in a bucket.

        :param bucket_name: Name of the bucket
        :param prefix: Filter objects by prefix (optional)
        :param recursive: List objects recursively (default is False)
        :return: Generator of objects
        """
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive)
            self.logger.debug(f"Listing objects in bucket '{bucket_name}'.")
            return objects
        except S3Error as e:
            self.logger.error(f"S3Error listing objects in bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")
            raise

    def get_bucket_tags(self, bucket_name: str) -> Dict[str, str]:
        """
        Get tags for a bucket.

        :param bucket_name: Name of the bucket
        :return: Dictionary of bucket tags
        """
        try:
            tags = self.client.get_bucket_tags(bucket_name)
            self.logger.debug(f"Retrieved tags for bucket '{bucket_name}'.")
            return tags
        except S3Error as e:
            self.logger.error(f"S3Error getting tags for bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error getting tags for bucket '{bucket_name}': {e}")
            raise

    # Object Operations
    def upload_file_stream(
            self, bucket_name: str, data_stream: io.BytesIO, object_name: str,
            content_type: str = 'application/octet-stream'
    ) -> None:
        """
        Upload a file to a specific bucket in MinIO using put_object() with a memory stream.

        :param bucket_name: Name of the bucket
        :param data_stream: Memory stream object containing the file data (e.g., io.BytesIO)
        :param object_name: Name to save the file as in the bucket
        :param content_type: MIME type of the file being uploaded (optional, default is 'application/octet-stream')
        """
        try:
            # Get the size of the data in the stream
            data_stream.seek(0, io.SEEK_END)
            file_size = data_stream.tell()
            data_stream.seek(0)

            self.client.put_object(bucket_name, object_name, data_stream, file_size, content_type=content_type)
            self.logger.debug(f"File '{object_name}' uploaded to bucket '{bucket_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error uploading file '{object_name}' to bucket '{bucket_name}': {e}")
            raise

        except Exception as e:
            self.logger.error(f"Error uploading file '{object_name}' to bucket '{bucket_name}': {e}")
            raise

    def upload_file(self, bucket_name: str, file_path: str, object_name: Optional[str] = None) -> None:
        """
        Upload a file to a specific bucket in MinIO.

        :param bucket_name: Name of the bucket
        :param file_path: Path to the file to upload
        :param object_name: Name to save the file as in the bucket (optional)
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        # Validate that the file exists
        if not os.path.isfile(file_path):
            self.logger.error(f"File '{file_path}' does not exist.")
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            self.logger.debug(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error uploading file '{file_path}' to bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error uploading file '{file_path}' to bucket '{bucket_name}': {e}")
            raise

    def download_file(
            self, bucket_name: str, object_name: str, file_path: str, force_create_folders: bool = True
    ) -> None:
        """
        Download a file from a specific bucket in MinIO.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object to download
        :param file_path: Path to save the downloaded file
        :param force_create_folders: Flag to create directories if they do not exist (default is True)
        """
        target_directory = os.path.dirname(file_path)

        # Validate and create target directories if necessary
        if not os.path.exists(target_directory):
            if force_create_folders:
                try:
                    os.makedirs(target_directory)
                    self.logger.info(f"Created target directory '{target_directory}'.")
                except Exception as e:
                    self.logger.error(f"Error creating target directory '{target_directory}': {e}")
                    raise
            else:
                self.logger.error(f"Destination folder structure '{target_directory}' does not exist.")
                return

        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            self.logger.debug(f"File '{object_name}' from bucket '{bucket_name}' downloaded to '{file_path}'.")
        except S3Error as e:
            self.logger.error(f"S3Error downloading file '{object_name}' from bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error downloading file '{object_name}' from bucket '{bucket_name}': {e}")
            raise

    def download_file_stream(self, bucket_name: str, object_name: str) -> io.BytesIO:
        """
        Download an object from a specific bucket in MinIO and return it as a memory stream.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object to download
        :return: A memory stream (io.BytesIO) containing the downloaded file data
        """
        try:
            # Retrieve the object from the bucket
            response = self.client.get_object(bucket_name, object_name)
            # Read the response data into a BytesIO stream
            data_stream = io.BytesIO(response.read())
            # Close the response (important to release network resources)
            response.close()
            response.release_conn()

            self.logger.debug(f"File '{object_name}' from bucket '{bucket_name}' downloaded as a memory stream.")
            return data_stream

        except S3Error as e:
            self.logger.error(f"S3Error downloading file '{object_name}' from bucket '{bucket_name}': {e}")
            raise

        except Exception as e:
            self.logger.error(f"Error downloading file '{object_name}' from bucket '{bucket_name}': {e}")
            raise

    def delete_object(self, bucket_name: str, object_name: str) -> None:
        """
        Delete a file from a specific bucket in MinIO.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object to delete
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            self.logger.warning(f"File '{object_name}' deleted from bucket '{bucket_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error deleting file '{object_name}' from bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error deleting file '{object_name}' from bucket '{bucket_name}': {e}")
            raise

    def delete_objects(self, bucket_name: str, objects: Union[Iterable[DeleteObject], List[Dict[str, Any]]]) -> None:
        """
        Delete multiple objects from a bucket.

        :param bucket_name: Name of the bucket
        :param objects: Either an Iterable of DeleteObject instances or a List of dictionaries,
                        each containing 'name' and optionally 'version_id'.
        """
        try:
            if all(isinstance(obj, DeleteObject) for obj in objects):
                # If all elements are instances of DeleteObject
                delete_objects_list = objects  # Use directly as it is already an Iterable[DeleteObject]
            elif all(isinstance(obj, dict) for obj in objects):
                # If all elements are dictionaries
                delete_objects_list = [
                    DeleteObject(obj["name"], obj.get("version_id"))
                    for obj in objects
                ]
            else:
                raise ValueError(
                    "Invalid object type provided. Must be either a list of DeleteObject instances or dictionaries.")

            delete_results = self.client.remove_objects(bucket_name, delete_objects_list)
            for del_err in delete_results:
                self.logger.error(f"Failed to delete object: {del_err}")
            self.logger.warning(f"Removed objects from bucket '{bucket_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error removing objects from bucket '{bucket_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error removing objects from bucket '{bucket_name}': {e}")
            raise

    def copy_object(self, bucket_name: str, object_name: str, source: str) -> None:
        """
        Copy an object from a source to a destination.

        :param bucket_name: Destination bucket name
        :param object_name: Destination object name
        :param source: Source of the object in the format 'bucket/object'
        """
        try:
            copy_source = CopySource(bucket_name=source.split('/')[0], object_name='/'.join(source.split('/')[1:]))
            self.client.copy_object(bucket_name, object_name, copy_source)
            self.logger.info(f"Copied object '{source}' to '{bucket_name}/{object_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error copying object '{source}' to '{bucket_name}/{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error copying object '{source}' to '{bucket_name}/{object_name}': {e}")
            raise

    def get_object_stats(self, bucket_name: str, object_name: str) -> Object:
        """
        Get metadata/statistics of an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :return: Object metadata as a dictionary
        """
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            self.logger.debug(f"Retrieved metadata for object '{object_name}' in bucket '{bucket_name}'.")
            return stat
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise ValueError(
                    f"Cannot access {object_name!r}: Invalid path or it does not correspond to a file object."
                ) from e
            raise
        except Exception as e:
            self.logger.error(f"Error getting metadata for object '{object_name}': {e}")
            raise

    def get_object_lock_configuration(self, bucket_name: str) -> Optional['ObjectLockConfig']:
        """
        Get the object lock configuration of a bucket.

        :param bucket_name: Name of the bucket
        :return: Object lock configuration as a dictionary, or None if not found
        :raises: ValueError if the bucket does not exist or cannot be accessed
        """
        try:
            lock_config = self.client.get_object_lock_config(bucket_name)
            self.logger.debug(f"Retrieved object lock configuration for bucket '{bucket_name}'.")
            return lock_config
        except S3Error as e:
            if e.code == "NoSuchBucket":
                raise ValueError(f"Bucket '{bucket_name}' does not exist or cannot be accessed.") from e
            elif e.code == "ObjectLockConfigurationNotFoundError":
                self.logger.warning(f"Bucket '{bucket_name}' does not have an object lock configuration.")
                return None
            else:
                self.logger.error(f"S3Error retrieving object lock configuration for bucket '{bucket_name}': {e}")
                raise
        except Exception as e:
            self.logger.error(f"Error getting object lock configuration for bucket '{bucket_name}': {e}")
            raise

    def get_object_tags(self, bucket_name: str, object_name: str) -> Dict[str, str]:
        """
        Get tags for an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :return: Dictionary of object tags
        """
        try:
            tags = self.client.get_object_tags(bucket_name, object_name)
            self.logger.debug(f"Retrieved tags for object '{object_name}' in bucket '{bucket_name}'.")
            return tags
        except S3Error as e:
            self.logger.error(f"S3Error getting tags for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error getting tags for object '{object_name}': {e}")
            raise

    def set_object_tags(self, bucket_name: str, object_name: str, tags: Union[Tags, Dict[str, str]]) -> None:
        """
        Set tags for an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :param tags: Either a Tags object or a dictionary of tags to set
        """
        try:
            if isinstance(tags, Tags):
                # If tags is already a Tags object
                tags_to_set = tags
            elif isinstance(tags, dict):
                # If tags is a dictionary, convert to Tags object
                tags_to_set = Tags(for_object=True)
                for key, value in tags.items():
                    tags_to_set[key] = value
            else:
                raise ValueError(
                    "Invalid tags type provided. Must be either a Tags object or a dictionary."
                )

            self.client.set_object_tags(bucket_name, object_name, tags_to_set)
            self.logger.info(f"Set tags for object '{object_name}' in bucket '{bucket_name}'.")
        except S3Error as e:
            self.logger.error(f"S3Error setting tags for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error setting tags for object '{object_name}': {e}")
            raise

    def get_object_retention(self, bucket_name: str, object_name: str) -> Optional[dict]:
        """
        Get retention settings for an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :return: Retention settings for the object as a dictionary
        """
        try:
            retention = self.client.get_object_retention(bucket_name, object_name)
            self.logger.debug(f"Retrieved retention for object '{object_name}' in bucket '{bucket_name}'.")
            return retention
        except S3Error as e:
            self.logger.error(f"S3Error getting retention for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error getting retention for object '{object_name}': {e}")
            raise

    def get_presigned_get_object_url(
            self, bucket_name: str, object_name: str, expires: Optional[timedelta] = None
    ) -> str:
        """
        Generate a presigned URL to download an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :param expires: Expiry time as timedelta (default to initialized 'default_presigned_urls_expiration_time')
        :return: Presigned URL for GET request
        """
        try:
            # If expires argument is not defined, use instance class value
            if expires is None:
                expires = self.default_presigned_expiration

            url = self.client.presigned_get_object(bucket_name, object_name, expires=expires)
            self.logger.debug(f"Generated presigned URL for GET request: {url}")
            return url
        except S3Error as e:
            self.logger.error(f"S3Error generating presigned GET URL for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error generating presigned GET URL for object '{object_name}': {e}")
            raise

    def get_presigned_put_object_url(
            self, bucket_name: str, object_name: str, expires: Optional[timedelta] = None
    ) -> str:
        """
        Generate a presigned URL to upload an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :param expires: Expiry time as timedelta (default to initialized 'default_presigned_urls_expiration_time')
        :return: Presigned URL for PUT request
        """
        try:
            # If expires argument is not defined, use instance class value
            if expires is None:
                expires = self.default_presigned_expiration

            url = self.client.presigned_put_object(bucket_name, object_name, expires=expires)
            self.logger.debug(f"Generated presigned URL for PUT request: {url}")
            return url
        except S3Error as e:
            self.logger.error(f"S3Error generating presigned PUT URL for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error generating presigned PUT URL for object '{object_name}': {e}")
            raise

    def get_presigned_delete_object_url(
            self, bucket_name: str, object_name: str, expires: Optional[timedelta] = None
    ) -> str:
        """
        Generate a presigned URL to delete an object.

        :param bucket_name: Name of the bucket
        :param object_name: Name of the object
        :param expires: Expiry time as timedelta (default to initialized 'default_presigned_urls_expiration_time')
        :return: Presigned URL for DELETE request
        """
        try:
            # If expires argument is not defined, use instance class value
            if expires is None:
                expires = self.default_presigned_expiration

            url = self.client.get_presigned_url("DELETE", bucket_name, object_name, expires=expires)
            self.logger.debug(f"Generated presigned URL for DELETE request: {url}")
            return url
        except S3Error as e:
            self.logger.error(f"S3Error generating presigned DELETE URL for object '{object_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error generating presigned DELETE URL for object '{object_name}': {e}")
            raise
