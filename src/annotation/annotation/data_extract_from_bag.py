import os
import json
import math
from sensor_msgs_py import point_cloud2 as pc2
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import sensor_msgs.msg
import bisect

import numpy as np
import cv2

reader = rosbag2_py.SequentialReader()
path = '/home/zeys/files/bags/zeyy/deepenai/all_camera4'

storage_option = rosbag2_py.StorageOptions(
    uri=path, storage_id="sqlite3")

converter_options = rosbag2_py.ConverterOptions(
    input_serialization_format='cdr',
    output_serialization_format='cdr')

reader.open(storage_option, converter_options)
msg_counter = 0
topic_types = reader.get_all_topics_and_types()
# Create a map for quicker lookup
type_map = {topic_types[i].name: topic_types[i].type for i in range(len(topic_types))}

camera_info_data = {}  # Dictionary to store camera info data for each camera topic
closest_image_ts = {}
lidar_ts = 0.0


def get_closest_index(arr, target):
    n = len(arr)
    left = 0
    right = n - 1
    mid = 0

    # edge case - last or above all
    if float(target) >= arr[n - 1]:
        return n - 1
    # edge case - first or below all
    if float(target) <= arr[0]:
        return 0
    # BSearch solution: Time & Space: Log(N)

    while left < right:
        mid = (left + right) // 2  # find the mid
        if float(target) < arr[mid]:
            right = mid
        elif float(target) > arr[mid]:
            left = mid + 1
        else:
            return mid

    def find_closest(ind1, ind2):
        return ind2 if float(target) - arr[ind1] >= arr[ind2] - float(target) else ind1

    if float(target) < arr[mid]:
        return find_closest(mid - 1, mid)
    else:
        return find_closest(mid, mid + 1)


def sync_timestamps_binary_search(lidar_timestamps, camera_data):
        lidar_timestamps.sort()  # Preprocess lidar timestamps (sort in ascending order)
        json_data['images'] = []  # Clear the previous image data

        for cam_key, cam_msg in camera_data.items():
            cam_ts = float(cam_key.split('_')[-1])
            closest_index = bisect.bisect_left(lidar_timestamps, cam_ts)
            if closest_index == len(lidar_timestamps):
                closest_ts = lidar_timestamps[-1]
            elif closest_index == 0:
                closest_ts = lidar_timestamps[0]
            else:
                prev_ts = lidar_timestamps[closest_index - 1]
                next_ts = lidar_timestamps[closest_index]
                closest_ts = prev_ts if cam_ts - prev_ts <= next_ts - cam_ts else next_ts

            json_data['images'].append({
                'timestamp': closest_ts,
            })

json_data = {
    "images": [],
    "timestamp": 0.0,
    "device_heading": {
        "x": 0.0,
        "y": 0.0,
        "z": 0.0,
        "w": 1.0
    },
    "points": [],
    "device_position": {
        "y": 0.0,
        "x": 0.310,
        "z": 1.700
    }
}

def process_pointcloud(topic, msg_type, msg, lidar_timestamps, camera_data):
    point_list = []
    for p in pc2.read_points(msg, field_names=("x", "y", "z", "intensity"), skip_nans=True):
        point_list.append({
            "x": float(p[0]),
            "y": float(p[1]),
            "z": float(p[2]),
            "i": float(p[3])
        })
    print(99)
    lidar_ts = float(msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9)
    if lidar_ts not in lidar_timestamps:
        lidar_timestamps.append(lidar_ts)

    json_data["points"] = point_list
    json_data["timestamp"] = lidar_ts
    return lidar_timestamps, camera_data


def process_image_compressed(topic, msg_type, msg, lidar_timestamps, camera_data):
    print(000)
    camera_id = topic.split('/')[-2]
    if msg_type == sensor_msgs.msg.CompressedImage:
        if isinstance(msg, sensor_msgs.msg.CompressedImage):
            camera_ts = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
            camera_data[f'{camera_id}_{camera_ts}'] = msg
    return lidar_timestamps, camera_data


topics = {
    '/sensing/lidar/concatenated/pointcloud': process_pointcloud,
    '/lucid_vision/camera_0/image_compressed': process_image_compressed,
}

lidar_timestamps = []
camera_data = {}

while reader.has_next():
    image_data = []
    (topic, data, t) = reader.read_next()
    msg_type = get_message(type_map[topic])
    msg = deserialize_message(data, msg_type)
    if topic in topics.keys():
        lidar_timestamps, camera_data = topics[topic](topic, msg_type, msg, lidar_timestamps, camera_data)


# to check not empyt files
if all([lidar_timestamps, camera_data]):
    timestamp_mapping = sync_timestamps_binary_search(lidar_timestamps, camera_data)
    # print(timestamp_mapping)
