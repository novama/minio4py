import io
import os
import unittest
from datetime import timedelta
from unittest.mock import patch, MagicMock

from minio import S3Error
from urllib3.response import BaseHTTPResponse

from minio4py.minio4py import Minio4Py, DeleteObject, Tags


# noinspection HttpUrlsUsage
class TestMinio4Py(unittest.TestCase):

    def setUp(self):
        """
        Set up test environment.
        """
        self.minio_host = 'localhost:9000'
        self.minio_access_key = 'test-access-key'
        self.minio_secret_key = 'test-secret-key'
        self.minio_secure = False
        self.default_presigned_expiration = timedelta(hours=1)  # 1 hour

        self.bucket_name = 'test-bucket'
        self.file_path = '/resources/test_file.txt'
        self.object_name = 'test-object.txt'
        self.download_path = '/downloads/test-object.txt'

        # Initialize Minio4Py instance
        self.minio4py = Minio4Py(
            self.minio_host,
            self.minio_access_key,
            self.minio_secret_key,
            self.minio_secure,
            self.default_presigned_expiration
        )

    @patch('minio4py.minio4py.Minio')
    def test_connect_to_minio(self, mock_minio):
        """
        Test connection to MinIO server.
        """
        minio4py = Minio4Py(
            self.minio_host,
            self.minio_access_key,
            self.minio_secret_key,
            self.minio_secure
        )

        mock_minio.assert_called_once_with(
            self.minio_host,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure
        )
        self.assertIs(minio4py.client, mock_minio.return_value)

    @patch('minio4py.minio4py.Minio.make_bucket')
    def test_create_bucket(self, mock_make_bucket):
        """
        Test creation of a new bucket.
        """
        self.minio4py.create_bucket(self.bucket_name)
        mock_make_bucket.assert_called_once_with(self.bucket_name)

    @patch('minio4py.minio4py.Minio.bucket_exists')
    def test_bucket_exists(self, mock_bucket_exists):
        """
        Test checking if a bucket exists.
        """
        mock_bucket_exists.return_value = True
        exists = self.minio4py.bucket_exists(self.bucket_name)
        mock_bucket_exists.assert_called_once_with(self.bucket_name)
        self.assertTrue(exists)

    @patch('minio4py.minio4py.Minio.remove_bucket')
    def test_remove_bucket(self, mock_remove_bucket):
        """
        Test removing a bucket.
        """
        self.minio4py.remove_bucket(self.bucket_name)
        mock_remove_bucket.assert_called_once_with(self.bucket_name)

    @patch('minio4py.minio4py.Minio.list_objects')
    def test_list_objects(self, mock_list_objects):
        """
        Test listing objects in a bucket.
        """
        mock_list_objects.return_value = [MagicMock(object_name=self.object_name)]
        objects = list(self.minio4py.list_objects(self.bucket_name))
        mock_list_objects.assert_called_once_with(self.bucket_name, prefix=None, recursive=False)
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].object_name, self.object_name)

    @patch('minio4py.minio4py.Minio.get_bucket_tags')
    def test_get_bucket_tags(self, mock_get_bucket_tags):
        """
        Test getting bucket tags.
        """
        mock_tags = {'key1': 'value1'}
        mock_get_bucket_tags.return_value = mock_tags
        tags = self.minio4py.get_bucket_tags(self.bucket_name)
        mock_get_bucket_tags.assert_called_once_with(self.bucket_name)
        self.assertEqual(tags, mock_tags)

    @patch('minio4py.minio4py.Minio.fput_object')
    @patch('os.path.isfile', return_value=True)
    def test_upload_file(self, _, mock_fput_object):
        """
        Test uploading a file to a bucket.
        """
        self.minio4py.upload_file(self.bucket_name, self.file_path, self.object_name)
        mock_fput_object.assert_called_with(self.bucket_name, self.object_name, self.file_path)

    @patch('os.path.isfile', return_value=False)
    def test_upload_file_not_exists(self, _):
        """
        Test uploading a file that does not exist.
        """
        with self.assertRaises(FileNotFoundError):
            self.minio4py.upload_file(self.bucket_name, self.file_path, self.object_name)

    @patch('minio4py.minio4py.Minio.fget_object')
    @patch('os.makedirs')
    @patch('os.path.exists', return_value=False)
    def test_download_file(self, _, mock_makedirs, mock_fget_object):
        """
        Test downloading a file from a bucket.
        """
        self.minio4py.download_file(self.bucket_name, self.object_name, self.download_path)
        mock_fget_object.assert_called_with(self.bucket_name, self.object_name, self.download_path)
        mock_makedirs.assert_called_once_with(os.path.dirname(self.download_path))

    @patch('minio4py.minio4py.Minio.remove_object')
    def test_delete_object(self, mock_remove_object):
        """
        Test deleting a file from a bucket.
        """
        self.minio4py.delete_object(self.bucket_name, self.object_name)
        mock_remove_object.assert_called_with(self.bucket_name, self.object_name)

    @patch('minio4py.minio4py.Minio.remove_objects')
    def test_delete_objects(self, mock_remove_objects):
        """
        Test deleting multiple objects from a bucket using a list of dicts.
        """
        objects = [{"name": self.object_name}]
        self.minio4py.delete_objects(self.bucket_name, objects)
        mock_remove_objects.assert_called_once()
        args, _ = mock_remove_objects.call_args
        delete_objects_list = args[1]
        self.assertIsInstance(delete_objects_list[0], DeleteObject)
        self.assertEqual(delete_objects_list[0]._name, self.object_name)

    @patch('minio4py.minio4py.Minio.remove_objects')
    def test_delete_objects_with_DeleteObject(self, mock_remove_objects):
        """
        Test deleting multiple objects from a bucket using a list of DeleteObject instances.
        """
        objects = [DeleteObject(self.object_name)]
        self.minio4py.delete_objects(self.bucket_name, objects)
        mock_remove_objects.assert_called_once_with(self.bucket_name, objects)

    @patch('minio4py.minio4py.Minio.copy_object')
    def test_copy_object(self, mock_copy_object):
        """
        Test copying an object from source to destination.
        """
        source = 'source-bucket/source-object'
        self.minio4py.copy_object(self.bucket_name, self.object_name, source)
        copy_source = mock_copy_object.call_args[0][2]
        self.assertEqual(copy_source.bucket_name, "source-bucket")
        self.assertEqual(copy_source.object_name, "source-object")

    @patch('minio4py.minio4py.Minio.stat_object')
    def test_get_object_stats(self, mock_stat_object):
        """
        Test getting metadata/statistics of an object.
        """
        mock_stat = MagicMock()
        mock_stat_object.return_value = mock_stat
        stat = self.minio4py.get_object_stats(self.bucket_name, self.object_name)
        mock_stat_object.assert_called_once_with(self.bucket_name, self.object_name)
        self.assertEqual(stat, mock_stat)

    @patch('minio4py.minio4py.Minio.stat_object')
    def test_get_object_stats_no_such_key(self, mock_stat_object):
        """
        Test getting metadata/statistics of an object that doesn't exist.
        """
        mock_stat_object.side_effect = S3Error(
            "NoSuchKey", "The specified key does not exist.", "", "", "",
            BaseHTTPResponse(
                status=500, version=1, version_string="1", reason="", request_url="", decode_content=False
            )
        )
        with self.assertRaises(ValueError):
            self.minio4py.get_object_stats(self.bucket_name, self.object_name)

    @patch('minio4py.minio4py.Minio.get_object_tags')
    def test_get_object_tags(self, mock_get_object_tags):
        """
        Test getting object tags.
        """
        mock_tags = {'key1': 'value1'}
        mock_get_object_tags.return_value = mock_tags
        tags = self.minio4py.get_object_tags(self.bucket_name, self.object_name)
        mock_get_object_tags.assert_called_once_with(self.bucket_name, self.object_name)
        self.assertEqual(tags, mock_tags)

    @patch('minio4py.minio4py.Minio.set_object_tags')
    def test_set_object_tags(self, mock_set_object_tags):
        """
        Test setting object tags.
        """
        tags = {'key1': 'value1', 'key2': 'value2'}
        self.minio4py.set_object_tags(self.bucket_name, self.object_name, tags)
        mock_set_object_tags.assert_called_once_with(self.bucket_name, self.object_name, tags)

    @patch('minio4py.minio4py.Minio.set_object_tags')
    def test_set_object_tags_with_Tags(self, mock_set_object_tags):
        """
        Test setting object tags using a Tags object.
        """
        # Convert the dictionary to a Tags object for comparison
        tags = Tags(for_object=True)
        tags["key1"] = "value1"
        tags["key2"] = "value2"
        self.minio4py.set_object_tags(self.bucket_name, self.object_name, tags)
        mock_set_object_tags.assert_called_once_with(self.bucket_name, self.object_name, tags)

    @patch('minio4py.minio4py.Minio.get_object_retention')
    def test_get_object_retention(self, mock_get_object_retention):
        """
        Test getting object retention settings.
        """
        mock_retention = MagicMock()
        mock_get_object_retention.return_value = mock_retention
        retention = self.minio4py.get_object_retention(self.bucket_name, self.object_name)
        mock_get_object_retention.assert_called_once_with(self.bucket_name, self.object_name)
        self.assertEqual(retention, mock_retention)

    @patch('minio4py.minio4py.Minio.presigned_get_object')
    def test_get_presigned_get_object_url(self, mock_presigned_get_object):
        """
        Test generating a presigned URL to download an object.
        """
        mock_url = 'http://example.com/presigned_get'
        mock_presigned_get_object.return_value = mock_url
        url = self.minio4py.get_presigned_get_object_url(self.bucket_name, self.object_name)
        mock_presigned_get_object.assert_called_once_with(
            self.bucket_name, self.object_name, expires=self.default_presigned_expiration
        )
        self.assertEqual(url, mock_url)

    @patch('minio4py.minio4py.Minio.presigned_put_object')
    def test_get_presigned_put_object_url(self, mock_presigned_put_object):
        """
        Test generating a presigned URL to upload an object.
        """
        mock_url = 'http://example.com/presigned_put'
        mock_presigned_put_object.return_value = mock_url
        url = self.minio4py.get_presigned_put_object_url(self.bucket_name, self.object_name)
        mock_presigned_put_object.assert_called_once_with(
            self.bucket_name, self.object_name, expires=self.default_presigned_expiration
        )
        self.assertEqual(url, mock_url)

    @patch('minio4py.minio4py.Minio.get_presigned_url')
    def test_get_presigned_delete_object_url(self, mock_get_presigned_url):
        """
        Test generating a presigned URL to delete an object.
        """
        mock_url = 'http://example.com/presigned_delete'
        mock_get_presigned_url.return_value = mock_url
        url = self.minio4py.get_presigned_delete_object_url(self.bucket_name, self.object_name)
        mock_get_presigned_url.assert_called_once_with(
            "DELETE", self.bucket_name, self.object_name, expires=self.default_presigned_expiration
        )
        self.assertEqual(url, mock_url)

    @patch('minio4py.minio4py.Minio.put_object')
    def test_upload_file_stream(self, mock_put_object):
        """
        Test uploading a file stream to a bucket.
        """
        file_data = b"file content"
        file_stream = io.BytesIO(file_data)
        object_name = 'test-object.txt'
        bucket_name = 'test-bucket'

        self.minio4py.upload_file_stream(
            bucket_name, file_stream, object_name, content_type='application/octet-stream')

        mock_put_object.assert_called_once_with(
            bucket_name, object_name, file_stream, len(file_data), content_type='application/octet-stream')

    @patch('minio4py.minio4py.Minio.get_object')
    def test_download_file_stream(self, mock_get_object):
        """
        Test downloading an object as a memory stream.
        """
        mock_response = MagicMock()
        mock_response.read.return_value = b"file content"
        mock_get_object.return_value = mock_response

        # Call the method under test
        result_stream = self.minio4py.download_file_stream('test-bucket', 'test-object.txt')

        # Assertions
        mock_get_object.assert_called_once_with('test-bucket', 'test-object.txt')
        mock_response.read.assert_called_once()
        self.assertEqual(result_stream.getvalue(), b"file content")
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

    @patch('minio4py.minio4py.Minio.get_object')
    def test_download_file_stream_s3_error(self, mock_get_object):
        """
        Test error handling when S3Error occurs during download.
        """
        mock_response = MagicMock()
        mock_get_object.side_effect = S3Error(
            "Test S3Error",  # error_code
            "Test Message",  # error_message
            "test-resource",  # resource
            "test-request-id",  # request_id
            "test-host-id",  # host_id
            mock_response  # response (a mock or actual HTTP response object)
        )

        with self.assertRaises(S3Error):
            self.minio4py.download_file_stream('test-bucket', 'test-object.txt')

    @patch('minio4py.minio4py.Minio.get_object')
    def test_download_file_stream_general_error(self, mock_get_object):
        """
        Test error handling for general errors during download.
        """
        mock_get_object.side_effect = Exception("General Error")

        with self.assertRaises(Exception):
            self.minio4py.download_file_stream('test-bucket', 'test-object.txt')

    @patch('minio4py.minio4py.Minio.get_object_lock_config')
    def test_get_object_lock_configuration_success(self, mock_get_object_lock_config):
        """
        Test successful retrieval of object lock configuration.
        """
        mock_lock_config = {"mode": "GOVERNANCE", "retainUntilDate": "2024-09-01T00:00:00Z"}
        mock_get_object_lock_config.return_value = mock_lock_config

        result = self.minio4py.get_object_lock_configuration('test-bucket')

        mock_get_object_lock_config.assert_called_once_with('test-bucket')
        self.assertEqual(result, mock_lock_config)

    @patch('minio4py.minio4py.Minio.get_object_lock_config')
    def test_get_object_lock_configuration_no_such_bucket(self, mock_get_object_lock_config):
        """
        Test handling when bucket does not exist during object lock configuration retrieval.
        """
        mock_response = MagicMock()
        mock_get_object_lock_config.side_effect = S3Error(
            "NoSuchBucket",  # error_code
            "Bucket does not exist",  # error_message
            "test-resource",  # resource
            "test-request-id",  # request_id
            "test-host-id",  # host_id
            mock_response  # response (a mock or actual HTTP response object)
        )

        with self.assertRaises(ValueError):
            self.minio4py.get_object_lock_configuration('test-bucket')

    @patch('minio4py.minio4py.Minio.get_object_lock_config')
    def test_get_object_lock_configuration_not_found(self, mock_get_object_lock_config):
        """
        Test handling when object lock configuration is not found.
        """
        mock_response = MagicMock()
        mock_get_object_lock_config.side_effect = S3Error(
            "ObjectLockConfigurationNotFoundError",  # error_code
            "No lock configuration found",  # error_message
            "test-resource",  # resource
            "test-request-id",  # request_id
            "test-host-id",  # host_id
            mock_response  # response (a mock or actual HTTP response object)
        )

        result = self.minio4py.get_object_lock_configuration('test-bucket')

        self.assertIsNone(result)

    @patch('minio4py.minio4py.Minio.get_object_lock_config')
    def test_get_object_lock_configuration_general_error(self, mock_get_object_lock_config):
        """
        Test error handling for general errors during object lock configuration retrieval.
        """
        mock_get_object_lock_config.side_effect = Exception("General Error")

        with self.assertRaises(Exception):
            self.minio4py.get_object_lock_configuration('test-bucket')


if __name__ == '__main__':
    unittest.main()
