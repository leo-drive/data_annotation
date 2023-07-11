import os
import json
import math
from cv_bridge import CvBridge
from sensor_msgs_py import point_cloud2 as pc2
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import sensor_msgs.msg
import yaml
import numpy as np
import cv2

reader = rosbag2_py.SequentialReader()
path = '/home/zeys/files/bags/zeyy/deepenai/denemebag_all_1//'
storage_option = rosbag2_py.StorageOptions(uri=path, storage_id="sqlite3")
converter_options = rosbag2_py.ConverterOptions(
    input_serialization_format='cdr',
    output_serialization_format='cdr')
reader.open(storage_option, converter_options)
# Set filter for topic of string type
storage_filter = rosbag2_py.StorageFilter(topics=['/sensing/lidar/concatenated/pointcloud', '/lucid_vision/camera_1/image_compressed'])
reader.set_filter(storage_filter)
topic_types = reader.get_all_topics_and_types()
# Create a map for quicker lookup
type_map = {topic_types[i].name: topic_types[i].type for i in range(len(topic_types))}


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
def sync_camera_lidar(camera_data,lidar_data):
    synchronized_timestamps = []
    camera_index=0
    image_data = []
    x=0
    bridge = CvBridge()
    # any time difference will be smaller tham min time diff
    for lidar_ts,lidar_msg in lidar_data.items():
        closest_camera_ts=None
        min_time_diff=float("inf")
        # Search for the closest camera timestamp starting from the current camera index
        for i in range(camera_index,len(camera_data)):
            time_diff=abs(lidar_ts-list(camera_data.keys())[i])
            if time_diff<min_time_diff:
                min_time_diff=time_diff
                closest_camera_ts=list(camera_data.keys())[i]
                image_msg= list(camera_data.values())[i]
        image_filename = f"{x}.png"
        image_cv=bridge.compressed_imgmsg_to_cv2(image_msg,desired_encoding="bgr8")
        height, width, _ = image_cv.shape
        img_np=np.frombuffer(image_cv,dtype=np.uint8).reshape(height,width,-1)
        image_path=os.path.join('/home/zeys/projects/data_annotation/camera_lidar_data/',f"{x}.png")
        cv2.imwrite(image_path, img_np)
        with open('/home/zeys/projects/golf_autoware/autoware/src/sensor_component/external/arena_camera/config/camera_f_info.yaml', 'r') as file:
            parameters = yaml.safe_load(file)
        image_data.append(
            {
            "fx": parameters['camera_matrix']['data'][0],
                "timestamp": closest_camera_ts,
                "p2": parameters['distortion_coefficients']['data'][3],
                "k1": parameters['distortion_coefficients']['data'][0],
                "p1": parameters['distortion_coefficients']['data'][2],
                "k3": parameters['distortion_coefficients']['data'][4],
                "k2": parameters['distortion_coefficients']['data'][1],
                "cy": parameters['camera_matrix']['data'][5],
                "cx": parameters['camera_matrix']['data'][2],
                "image_url": f"{x}.png",
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
        json_data["images"] = image_data
        json_data["timestamp"] = lidar_ts
        json_data["points"] = lidar_msg
        json_filename = f"{x}.json"
        json_path = os.path.join("/home/zeys/projects/data_annotation/camera_lidar_data/", json_filename)
        with open(json_path, "w") as json_file:
            json.dump(json_data, json_file, indent=4)
        x+=1
        image_data=[]
        # print("Lidar ts: ",lidar_ts,"Camera ts: ",closest_camera_ts)
        # synchronized_timestamps.append((lidar_ts,closest_camera_ts))
    return synchronized_timestamps

def process_image_compressed(msg,camera_data):
    camera_ts = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
    camera_data[camera_ts] = msg
    return camera_data

def process_pointcloud( msg, lidar_data):
    point_list = []
    for p in pc2.read_points(msg, field_names=("x", "y", "z", "intensity"), skip_nans=True):
        point_list.append({
            "x": float(p[0]),
            "y": float(p[1]),
            "z": float(p[2]),
            "i": float(p[3])
        })
    lidar_ts = float(msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9)
    lidar_data[lidar_ts] = point_list
    return lidar_data


lidar_data = {}
camera_data={}
synced_timestamps=[]

while reader.has_next():

    (topic, data, t) = reader.read_next()
    msg_type = get_message(type_map[topic])
    msg = deserialize_message(data, msg_type)
    if topic == "/sensing/lidar/concatenated/pointcloud":
        lidar_data=process_pointcloud(msg, lidar_data)
        # counter+=1
    if topic == "/lucid_vision/camera_1/image_compressed":
        camera_data=process_image_compressed(msg,camera_data)
        # counter+=1
# to check not empyt files
if all([lidar_data, camera_data]):
    synced_timestamps=sync_camera_lidar(camera_data,lidar_data)
