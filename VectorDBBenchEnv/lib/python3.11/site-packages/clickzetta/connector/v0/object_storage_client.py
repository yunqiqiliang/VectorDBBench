import enum
import io
import json


class ObjectStorageType(enum.Enum):
    OSS = 0
    COS = 1
    S3 = 2
    GCS = 3
    TOS = 4


class ObjectStorageClient(object):
    def __init__(
        self, objectStorageType, ak_id, ak_secret, token, endpoint, bucket=None
    ):
        self.type = objectStorageType
        self.ak_id = ak_id
        self.ak_secret = ak_secret
        self.token = token
        self.endpoint = endpoint
        self.bucket = bucket
        self.client = None
        if self.type == ObjectStorageType.OSS:
            import oss2

            auth = oss2.StsAuth(self.ak_id, self.ak_secret, self.token)
            self.client = oss2.Bucket(auth, self.endpoint, self.bucket)
        elif self.type == ObjectStorageType.COS:
            from qcloud_cos import CosConfig, CosS3Client

            cos_config = CosConfig(
                Region=self.endpoint,
                SecretId=self.ak_id,
                SecretKey=self.ak_secret,
                Token=self.token,
            )
            self.client = CosS3Client(cos_config)
        elif self.type == ObjectStorageType.S3:
            import boto3

            session = boto3.Session(
                aws_session_token=self.token,
                aws_access_key_id=self.ak_id,
                aws_secret_access_key=self.ak_secret,
                region_name=self.endpoint,
            )
            self.client = session.resource("s3")
        elif self.type == ObjectStorageType.GCS:
            from google.cloud import storage
            from google.oauth2 import service_account
            from google.oauth2.credentials import Credentials

            credentials = None
            if self.ak_id is not None and len(self.ak_id) > 0:
                credentials = service_account.Credentials.from_service_account_info(
                    json.loads(self.ak_id)
                )
            elif self.token is not None and len(self.token) > 0:
                credentials = Credentials(token=self.token)
            self.client = storage.Client(
                project="clickzetta-connector", credentials=credentials
            )
        elif self.type == ObjectStorageType.TOS:
            import tos
            self.client = tos.TosClientV2(ak_id, ak_secret, region=endpoint, security_token=token)
        else:
            raise Exception(f"Unsupported object storage type: {objectStorageType}")

    def get_stream(self, bucket_name, object_name):
        stream = None
        if self.type == ObjectStorageType.OSS:
            stream = self.client.get_object(object_name).read()
        elif self.type == ObjectStorageType.COS:
            stream = self.client.get_object(Bucket=bucket_name, Key=object_name)[
                "Body"
            ].get_raw_stream()
        elif self.type == ObjectStorageType.S3:
            stream = self.client.Object(bucket_name, object_name).get()["Body"].read()
        elif self.type == ObjectStorageType.GCS:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            file_obj = io.BytesIO()
            blob.download_to_file(file_obj)
            file_obj.seek(0)
            stream = file_obj
        elif self.type == ObjectStorageType.TOS:
            stream = self.client.get_object(bucket_name, object_name).read()
        return stream
