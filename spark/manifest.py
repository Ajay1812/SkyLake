import json
import boto3

class ManifestManager:
    def __init__(self, bucket, region, raw_prefix="raw/", manifest_key="raw/_manifest.json"):
        self.bucket = bucket
        self.raw_prefix = raw_prefix
        self.manifest_key = manifest_key
        self.s3 = boto3.client("s3", region_name=region)

    def _list_raw_csv_files(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.raw_prefix)
        files = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                files.append(key.split("/")[-1])
        return files

    def _read_processed(self):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.manifest_key)
            return json.loads(obj["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return []

    def get_new_files(self):
        processed = set(self._read_processed())
        all_files = set(self._list_raw_csv_files())
        return sorted(all_files - processed)

    def mark_processed(self, filenames):
        processed = set(self._read_processed())
        processed.update(filenames)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self.manifest_key,
            Body=json.dumps(sorted(processed)).encode("utf-8"),
        )
