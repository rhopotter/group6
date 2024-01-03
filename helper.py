from google.cloud import storage
 
def upload_to_cloud_storage(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
 
    # Upload the file to Cloud Storage
    blob.upload_from_filename(source_file_path)
 
    print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
 
if __name__ == "__main__":
    # Set your Google Cloud Storage bucket name
    bucket_name = "assesment_dataset"  # Replace with your actual bucket name
 
    # Set the local path of the CSV file downloaded using wget
    source_file_path = "Hydra-Movie-Scrape.csv"
 
    # Set the desired destination blob name in the bucket
    destination_blob_name = "MoviesDataset.csv"  # Replace with your desired destination blob name
 
    # Call the function to upload the file
    upload_to_cloud_storage(bucket_name, source_file_path, destination_blob_name)