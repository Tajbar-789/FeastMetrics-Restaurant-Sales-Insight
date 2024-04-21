from s3_client_object import S3ClientProvider
from config import load_config
from encryption_decryption import encrypt,decrypt

config = load_config(section="encryption")
s3_client=S3ClientProvider(decrypt(config["access_key"][1:-1]),decrypt(config["secret_access_key"][1:-1]))

s3_client_object=s3_client.get_client()

c1=load_config(section="s3_details")

s3_bucket_name=c1["bucket_name"]

path_to_download=c1["path_to_download"]
list_of_files=c1["list_of_files"][1:-1]
for file in list_of_files.split(","):
    print(file)
    try:
        s3_client_object.download_file(s3_bucket_name, file, path_to_download)
    except Exception as e:
        error_message = f"Error downloading file '{file}': {str(e)}"
        raise e