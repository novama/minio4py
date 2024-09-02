# Minio4Py - MinIO Python Client Wrapper

## Overview

This project is a Python-based wrapper around the MinIO Client, designed to simplify interactions with a MinIO server. The `Minio4Py` class provides an easy-to-use interface for common operations such as creating buckets, uploading and downloading files, managing object metadata, generating presigned URLs, and more.

The project includes robust error handling and logging, making it suitable for use in production environments where reliability and transparency are critical.

## Features

- **Bucket Operations:**
  - Create and remove buckets.
  - Check if a bucket exists.
  - List buckets and objects within a bucket.
  - Manage bucket tags.

- **Object Operations:**
  - Upload and download files.
  - Delete single or multiple objects.
  - Copy objects between buckets.
  - Retrieve and set object metadata (tags, retention).
  - Generate presigned URLs for GET, PUT, and DELETE operations.

- **Presigned URLs:**
  - Generate presigned URLs for downloading, uploading, and deleting objects with customizable expiration times.

## Requirements

- Python 3.6 or higher
- MinIO Python Client (`minio`) version 7.2.7 or later

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/minio-client-wrapper.git
   cd minio-client-wrapper
   ```

2. **Create a virtual environment (optional but recommended):**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Initializing the Client

To start using the MinIO client, first, initialize the `Minio4Py` class with your MinIO server's details:

```python
from minio4py import Minio4Py

minio4py = Minio4Py(
    host='localhost:9000',
    access_key='your-access-key',
    secret_key='your-secret-key',
    secure=False,  # Set to True if using HTTPS
    default_presigned_urls_expiration_time=3600  # 1 hour
)
```

### Example Operations

#### Creating a Bucket

```python
minio4py.create_bucket('my-bucket')
```

#### Uploading a File

```python
minio4py.upload_file('my-bucket', '/path/to/myfile.txt')
```

#### Downloading a File

```python
minio4py.download_file('my-bucket', 'myfile.txt', '/path/to/downloadedfile.txt')
```

#### Generating a Presigned GET URL

```python
url = minio4py.get_presigned_get_object_url('my-bucket', 'myfile.txt')
print(url)
```

### Error Handling

All methods in the `Minio4Py` class raise exceptions when operations fail. For example:

```python
try:
    minio4py.create_bucket('existing-bucket')
except Exception as e:
    print(f"Error: {e}")
```

### Logging

The `Minio4Py` class uses Python's built-in logging module to log important events and errors. You can customize the logging level by configuring the `logging` module in your application.

## Unit Tests

The project includes a comprehensive suite of unit tests covering all functionalities of the `Minio4Py` class.

### Running Tests

To run the unit tests, use the following command:

```bash
 pytest ./tests/test_minio4py.py
```

This will execute all the tests and provide a report of the results.

## Contributing

Contributions are welcome! If you find a bug or want to add new features, please create a pull request. Ensure that your changes are well-documented and include relevant tests.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.