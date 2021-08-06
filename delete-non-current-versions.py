import boto3
import pprint
import sys

# This is a simplified version of https://wasabi-support.zendesk.com/hc/en-us/articles/360058028992-How-do-I-mass-delete-non-current-versions-inside-a-bucket
# and assumes it will be run on an EC2 instances with an appropriate IAM role to work with the S3 bucket/objects in question.

# initialize basic variables for in memory storage.
delete_marker_count = 0
versioned_object_count = 0
current_object_count = 0
delete_marker_list = []
version_list = []
BUCKET = input("$ Please enter the name of the bucket: ").strip()
PREFIX = input("$ Please enter a prefix (leave blank if you don't need one)").strip()

pp = pprint.PrettyPrinter(indent=4)

s3 = boto3.client('s3')

# test its working
s3.list_buckets()

object_response_paginator = s3.get_paginator('list_object_versions')

print("$ Calculating, please wait... this may take a while")
for object_response_itr in object_response_paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    if 'DeleteMarkers' in object_response_itr:
        for delete_marker in object_response_itr['DeleteMarkers']:
            delete_marker_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})
            delete_marker_count += 1
    if 'Versions' in object_response_itr:
        for version in object_response_itr['Versions']:
            if version['IsLatest'] is False:
                versioned_object_count += 1
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
            elif version['IsLatest'] is True:
                current_object_count += 1
print("\n")
print("-" * 10)
print("\n")
pp.pprint(version_list)
print("\n")
print("-" * 10)
print("$ Total Delete markers: " + str(delete_marker_count))
print("$ Number of Current objects: " + str(current_object_count))
print("$ Number of Non-current objects: " + str(versioned_object_count))
print("-" * 10)

delete_flag = False
while not delete_flag:
    choice = input("$ Do you wish to delete the delete markers and non-current objects? [y/n]")
    if choice.strip().lower() == 'y':
        delete_flag = True
        print("$ starting deletes now...")
        print("$ removing delete markers 1000 at a time")
        for i in range(0, len(delete_marker_list), 1000):
            response = s3.delete_objects(
                Bucket=BUCKET,
                Delete={
                    'Objects': delete_marker_list[i:i + 1000],
                    'Quiet': True
                }
            )
            print(response)
        print("$ removing old versioned objects 1000 at a time")
        for i in range(0, len(version_list), 1000):
            response = s3.delete_objects(
                Bucket=BUCKET,
                Delete={
                    'Objects': version_list[i:i + 1000],
                    'Quiet': True
                }
            )
            print(response)

    elif choice == 'n':
        sys.exit()

    else:
        print("$ invalid choice please try again.")

print("$ process completed successfully")
print("\n")
print("\n")