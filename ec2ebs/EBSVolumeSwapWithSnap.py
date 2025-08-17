import boto3
import time
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime
import argparse
import threading
import sys

# Thread-safe print
lock = threading.Lock()

# Step 1: Parse CLI arguments
parser = argparse.ArgumentParser()
parser.add_argument("var_region")
parser.add_argument("instance_id")
parser.add_argument("volume_ids")
args = parser.parse_args()

REGION = args.var_region
INSTANCE_ID = args.instance_id
volume_ids_csv = args.volume_ids 

# Step 2: Initialize Boto3 clients/resources with dynamic region
dynamodb_client = boto3.client('dynamodb', region_name=REGION)
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION)
ec2 = boto3.client("ec2", region_name=REGION)
ec2_resource = boto3.resource("ec2", region_name=REGION)

# Step 3: Table names
OLD_TABLE_NAME = 'EBSVolumeSwapBeforeSnapshot'
NEW_TABLE_NAME = 'EBSVolumeSwapApplySnapshot'

# Step 4: Create DynamoDB tables if not exist
def create_table_if_not_exists(table_name, key_name):
    try:
        existing = dynamodb_client.list_tables()["TableNames"]
        if table_name in existing:
            print(f"[INFO] Table '{table_name}' already exists.")
            return

        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': key_name, 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': key_name, 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 26, 'WriteCapacityUnits': 26}
        )
        print(f"[INFO] Table '{table_name}' created.")
    except ClientError as e:
        print(f"[ERROR] Failed to create table {table_name}: {e}")

create_table_if_not_exists(OLD_TABLE_NAME, 'old_volume_id')
create_table_if_not_exists(NEW_TABLE_NAME, 'new_volume')

# Step 5: Access table resources
old_table = dynamodb_resource.Table(OLD_TABLE_NAME)
new_table = dynamodb_resource.Table(NEW_TABLE_NAME)

# Step 6: Logging functions
def record_old_volume(volume_id, instance_id, instance_name, device_name, size, iops, throughput, snapshot_id):
    try:
        old_table.put_item(
            Item={
                "old_volume_id": volume_id,
                "name": f"OLD-{volume_id}",
                "instancename": instance_name,
                "instance_id": instance_id,
                "devicename": device_name,
                "size": int(size),
                "iops": int(iops),
                "throughput": int(throughput),
                "snapshotid": snapshot_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        print(f"[INFO] Logged OLD volume {volume_id} into DynamoDB.")
    except Exception as e:
        print(f"[ERROR] Failed to log old volume {volume_id}: {e}")

def record_new_volume(new_volume_id, snapshot_id, instance_id, instance_name, device_name,
                      size, iops, throughput, volume_type, az, sg_names):
    try:
        new_table.put_item(
            Item={
                "new_volume": new_volume_id,
                "source_snapshot": snapshot_id,
                "instance_id": instance_id,
                "instancename": instance_name,
                "devicename": device_name,
                "size": int(size),
                "iops": int(iops),
                "throughput": int(throughput),
                "volume_type": volume_type,
                "availability_zone": az,
                "security_group_names": sg_names,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        print(f"[INFO] Logged NEW volume {new_volume_id} into DynamoDB.")
    except Exception as e:
        print(f"[ERROR] Failed to log new volume {new_volume_id}: {e}")

# Step 7: Utility functions
EXCLUDE_DEVICES = ["/dev/sda1", "/dev/xvda"]

def merge_tags(base_tags, new_tags):
    tag_dict = {tag["Key"]: tag["Value"] for tag in base_tags}
    for tag in new_tags:
        tag_dict[tag["Key"]] = tag["Value"]
    return [{"Key": k, "Value": v} for k, v in tag_dict.items()]

def get_filtered_attached_volumes(instance_id, volume_ids_csv):
    try:
        volume_ids = [v.strip() for v in volume_ids_csv.strip().split("|") if v.strip()]
        response = ec2.describe_instances(InstanceIds=[instance_id])
        volumes = []

        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                for mapping in instance.get("BlockDeviceMappings", []):
                    device = mapping.get("DeviceName")
                    volume_id = mapping.get("Ebs", {}).get("VolumeId")
                    if device not in EXCLUDE_DEVICES and volume_id in volume_ids:
                        volumes.append({
                            "volume_id": volume_id,
                            "device_name": device
                        })

        return volumes

    except ClientError as e:
        print(f"[ERROR] Unable to fetch volumes for instance {instance_id}: {e}")
        return []

def get_attached_volumes(instance_id):
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        volumes = []

        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                for mapping in instance.get("BlockDeviceMappings", []):
                    device = mapping.get("DeviceName")
                    volume_id = mapping.get("Ebs", {}).get("VolumeId")
                    if device not in EXCLUDE_DEVICES:
                        volumes.append({
                            "volume_id": volume_id,
                            "device_name": device
                        })

        return volumes

    except ClientError as e:
        print(f"[ERROR] Unable to fetch volumes for instance {instance_id}: {e}")
        return []

# Step 8: Snapshot and swap function
def snapshot_and_swap(volume_info):
    volume_id = volume_info["volume_id"]
    device_name = volume_info["device_name"]
    try:
        print(f"[INFO] Processing volume {volume_id} ({device_name})")

        volume = ec2_resource.Volume(volume_id)
        volume.load()
        az = volume.availability_zone
        size = volume.size
        iops = volume.iops or 0
        throughput = getattr(volume, "throughput", 0) or 0
        volume_type = volume.volume_type
        encrypted = volume.encrypted
        kmskey = volume.kms_key_id
        tags = volume.tags or []

        instance_info = ec2.describe_instances(InstanceIds=[INSTANCE_ID])["Reservations"][0]["Instances"][0]
        sg_names = ",".join([sg["GroupName"] for sg in instance_info.get("SecurityGroups", [])])
        name_tag = next((tag["Value"] for tag in instance_info.get("Tags", []) if tag["Key"] == "Name"), "Unknown")

        snapshot = ec2.create_snapshot(
            VolumeId=volume_id,
            Description=f"Snapshot of {volume_id} before swap"
        )
        snapshot_id = snapshot["SnapshotId"]
        print(f"[INFO] Snapshot {snapshot_id} initiated.")
        print(f"[INFO] KMS Key used: {kmskey}")
        ec2.get_waiter("snapshot_completed").wait(SnapshotIds=[snapshot_id])
        print(f"[INFO] Snapshot {snapshot_id} completed.")

        snapshot_tags = merge_tags(tags, [
            {"Key": "Name", "Value": f"Snapshot-of-{volume_id}"},
            {"Key": "device_name", "Value": device_name},
            {"Key": "VolumeType", "Value": volume_type},
            {"Key": "Size", "Value": str(size)},
            {"Key": "IOPS", "Value": str(iops)},
            {"Key": "Throughput", "Value": str(throughput)},
            {"Key": "AvailabilityZone", "Value": az},
            {"Key": "SecurityGroupName", "Value": sg_names},
            {"Key": "kms_key_id", "Value": str(kmskey)},
        ])
        ec2.create_tags(Resources=[snapshot_id], Tags=snapshot_tags)
        record_old_volume(volume_id, INSTANCE_ID, name_tag, device_name, size, iops, throughput, snapshot_id)

        extra_tags = merge_tags(tags, [
            {"Key": "Name", "Value": f"Recreated-{volume_id}"},
            {"Key": "SourceSnapshot", "Value": snapshot_id}
        ])

        new_volume = ec2.create_volume(
            SnapshotId=snapshot_id,
            AvailabilityZone=az,
            VolumeType=volume_type,
            Encrypted=encrypted,
            Iops=iops,
            Throughput=throughput,
            Size=size,
            KmsKeyId=kmskey,
            TagSpecifications=[{
                "ResourceType": "volume",
                "Tags": extra_tags
            }]
        )
        new_volume_id = new_volume["VolumeId"]
        print(f"[INFO] New volume {new_volume_id} creation started...")
        ec2.get_waiter("volume_available").wait(VolumeIds=[new_volume_id])
        print(f"[INFO] New volume {new_volume_id} is now available.")

        record_new_volume(new_volume_id, snapshot_id, INSTANCE_ID, name_tag, device_name,
                          size, iops, throughput, volume_type, az, sg_names)

        print(f"[INFO] Detaching old volume {volume_id}...")
        ec2.detach_volume(VolumeId=volume_id, InstanceId=INSTANCE_ID, Force=True  )
        ec2.get_waiter("volume_available").wait(VolumeIds=[volume_id]) 
        print(f"[INFO] Old volume {volume_id} detached.")

        print(f"[INFO] Attaching new volume {new_volume_id} to device {device_name}...")
        ec2.attach_volume(
            VolumeId=new_volume_id,
            InstanceId=INSTANCE_ID,
            Device=device_name
        )
        ec2.create_tags(
            Resources=[volume_id],
            Tags=[{"Key": "Name", "Value": f"OLD-{volume_id}"}]
        )
        print(f"[SUCCESS] Volume {volume_id} swapped with {new_volume_id} using snapshot {snapshot_id}. Size: {size} GiB")

    except WaiterError as we:
        print(f"[ERROR] Waiter failed for volume {volume_id}: {we}")
        sys.exit(1)
    except ClientError as e:
        print(f"[ERROR] AWS client error on volume {volume_id}: {e}")
        sys.exit(1)
    except Exception as ex:
        print(f"[ERROR] Unexpected error on volume {volume_id}: {ex}")
        sys.exit(1)
        #raise

# Step 9: Main driver
def main():
    print("Inside script")
    print(REGION)
    print(INSTANCE_ID)
    print(volume_ids_csv)
    if volume_ids_csv and volume_ids_csv != "AllVolumes" and volume_ids_csv.strip():
        all_volumes = get_filtered_attached_volumes(INSTANCE_ID, volume_ids_csv)
        print("filtered volume ids")
    else:
        all_volumes = get_attached_volumes(INSTANCE_ID)
        print("All volumes")
    
    print(all_volumes)

    if not all_volumes:
        print("[WARN] No eligible volumes found.")
        return

    print("[INFO] The following volumes will be processed:")
    for v in all_volumes:
        print(f"  - {v['volume_id']} ({v['device_name']})")

    with ThreadPoolExecutor(max_workers=len(all_volumes)) as executor:
        executor.map(snapshot_and_swap, all_volumes)

    print("[INFO] All volume operations completed.")

if __name__ == "__main__":
    main()
