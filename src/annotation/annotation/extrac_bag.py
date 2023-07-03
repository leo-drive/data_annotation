import os
import json
import math
from sensor_msgs_py import point_cloud2 as pc2
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import sensor_msgs.msg
import yaml
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

count=0
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
    for cam_key, cam_msg in camera_data.items():
        cam_ts = cam_key.split('_')[-1]
        closest_index = get_closest_index(lidar_timestamps, cam_ts)
        closest_ts = lidar_timestamps[closest_index]
        with open('/home/zeys/projects/golf_autoware/autoware/src/sensor_component/external/arena_camera/config/camera_fr_info.yaml', 'r') as file:
            parameters = yaml.safe_load(file)
        json_data['images'].append(
                {
                    "fx": parameters['camera_matrix']['data'][0],
                    "timestamp": closest_ts,
                    "p2": parameters['distortion_coefficients']['data'][3],
                    "k1": parameters['distortion_coefficients']['data'][0],
                    "p1": parameters['distortion_coefficients']['data'][2],
                    "k3": parameters['distortion_coefficients']['data'][4],
                    "k2": parameters['distortion_coefficients']['data'][1],
                    "cy": parameters['camera_matrix']['data'][5],
                    "cx": parameters['camera_matrix']['data'][2],
                    "image_url": f"{count}.png",
                    "fy": parameters['camera_matrix']['data'][4],
                    "position": {
                        "y": -0.006,
                        "x": 1.520,
                        "z": 1.529
                    },
                    "heading": {
                        "y": 0.497,
                        "x": -0.500,
                        "z": -0.496,
                        "w": 0.506
                    },
                    "camera_model": "pinhole"
                }
         )

        # json_data['images'].append(
        #     {
        #         # 'lorem_key': 'lorem_val',
        #         # 'message': cam_msg,
        #         'timestamp': closest_ts,
        #     }
        # )

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
        break

    lidar_ts = float(msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9)
    if lidar_ts not in lidar_timestamps:
        lidar_timestamps.append(lidar_ts)

    json_data["points"] = point_list
    json_data["timestamp"] = lidar_ts
    return lidar_timestamps, camera_data


def process_image_compressed(topic, msg_type, msg, lidar_timestamps, camera_data):
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
