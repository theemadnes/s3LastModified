# import supporting modules
from boto.s3.connection import S3Connection
import datetime
import csv

# create connection to s3 - this assumes access keys have been preconfigured using CLI / IAM role
conn = S3Connection()

# set the logging 
file_name = "last_modified_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
log_writer = csv.writer(open(file_name, "wb"))
# put a header at the top of the file
log_writer.writerow(["Bucket Name", "Key Name", "Last Modified Date"])

# get all of the buckets for this account and extract the bucket names
all_buckets = conn.get_all_buckets()
for a_bucket in all_buckets:
    # go to each bucket and get all necessary detail
    current_bucket = conn.get_bucket(a_bucket.name, validate=False)
    current_bucket_keys = current_bucket.list()
    key_count = 0;

    for key in current_bucket_keys:
        print key.name.encode('utf-8') + '' + key.last_modified
        key_count += 1
        print "Current key count in bucket " + current_bucket.name + ": " + str(key_count)
        log_writer.writerow([current_bucket.name, key.name.encode('utf-8'), key.last_modified])
