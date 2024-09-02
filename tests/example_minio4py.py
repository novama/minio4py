import os
import sys
from datetime import timedelta
from dotenv import load_dotenv
from minio4py.minio4py import Minio4Py, DeleteObject, Tags
from minio4py.minio4py import minio_path_join, minio_path_norm

# Get the directory of the current running file
BASE_PATH = os.path.dirname(os.path.realpath(__file__))
# .env file path (in the current base path)
env_file_path = os.path.join(BASE_PATH, '.env')


def validate_env_file():
    # Check if the .env file exists
    if not os.path.isfile(env_file_path):
        # Display error message if the .env file does not exist
        print(
            f"Error: The '{env_file_path}' file does not exist. Please make sure to configure and provide a valid "
            f".env file. Execution terminated."
        )
        # Stop the execution
        sys.exit(1)


# Call the function to validate the .env file
validate_env_file()
# Load .env file with environment variables
load_dotenv(dotenv_path=env_file_path)


class ExampleMinio4Py:

    @staticmethod
    def main():
        """
        Main function to demonstrate MinIO operations using Minio4Py class.
        """
        # Configuration variables
        minio_host = os.getenv("MINIO_HOST")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        minio_secure = os.getenv("MINIO_SECURE", "false").lower() in ["true", "1", "t", "yes"]
        default_presigned_expiration_time_hours = int(os.getenv("MINIO_PRESIGNED_EXPIRATION_TIME_HOURS", "1"))

        # Initialize Minio4Py
        minio4py = Minio4Py(
            minio_host,
            minio_access_key,
            minio_secret_key,
            minio_secure,
            timedelta(hours=default_presigned_expiration_time_hours)
        )

        # Example usage
        bucket_name = "my-test-bucket"
        objects_base_path = "/"
        file_path = "resources\\test_file.txt"  # wrong path on purpose to normalize
        object_name = "test_file-uploaded.txt"
        download_path = "./downloads/test_file-downloaded.txt"

        # Normalizing object path name (wrong path separators)
        file_path = minio_path_norm(file_path)
        # Normalizing object path name (correct path separators)
        file_path = minio_path_norm(file_path)

        minio4py.create_bucket(bucket_name)
        minio4py.list_buckets()
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, object_name))
        minio4py.download_file(bucket_name, minio_path_join(objects_base_path, object_name), download_path)
        minio4py.delete_object(bucket_name, minio_path_join(objects_base_path, object_name))

        # Adding test files
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object1.txt"))
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object2.txt"))
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object3.txt"))
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object4.txt"))

        # Example of tagging an object
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object1.txt"))
        minio4py.upload_file(bucket_name, file_path, minio_path_join(objects_base_path, "object2.txt"))
        # Using a Tags object
        tags_object = Tags(for_object=True)
        tags_object["key1"] = "value1"
        tags_object["key2"] = "value2"
        minio4py.set_object_tags(bucket_name, minio_path_join(objects_base_path, "object1.txt"), tags_object)
        # Using a dictionary
        tags_dict = {
            "key1": "value1",
            "key2": "value2"
        }
        minio4py.set_object_tags(bucket_name, minio_path_join(objects_base_path, "object2.txt"), tags_dict)

        # Example of deleting multiple objects
        # Using DeleteObject instances
        objects_to_delete = [
            DeleteObject(minio_path_join(objects_base_path, "object1.txt"), "version1"),
            DeleteObject(minio_path_join(objects_base_path, "object2.txt"))
        ]
        # Delete using DeleteObject instances
        minio4py.delete_objects(bucket_name, objects_to_delete)
        # Using dictionaries
        objects_to_delete_dicts = [
            {"name": minio_path_join(objects_base_path, "object3.txt"), "version_id": "version1"},
            {"name": minio_path_join(objects_base_path, "object4.txt")}
        ]
        # Delete using dictionaries
        minio4py.delete_objects(bucket_name, objects_to_delete_dicts)


if __name__ == '__main__':
    ExampleMinio4Py.main()
