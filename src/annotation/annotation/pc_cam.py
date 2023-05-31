import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2, Image
from message_filters import ApproximateTimeSynchronizer, Subscriber
from sensor_msgs_py import point_cloud2 as pc2
import numpy as np
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import os
import json
import yaml
import cv2


class PointcloudCamera(Node):

    def __init__(self):
        super().__init__('pointcloud_camera')
        qos_profile = rclpy.qos.qos_profile_sensor_data
        self.pointcloud_sub= Subscriber(self, PointCloud2, '/sensing/lidar/concatenated/pointcloud', qos_profile=qos_profile)
        self.image_sub = Subscriber(self, Image, '/lucid_vision/camera_1/image', qos_profile=qos_profile)
        self.approx_sync = ApproximateTimeSynchronizer([self.pointcloud_sub, self.image_sub], queue_size=10, slop=1)
        self.approx_sync.registerCallback(self.callback)
        self.json_data = {
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
                "x": 0.0,
                "z": 0.0
            }
        }
        self.msg_count = 0
        with open('/home/zeys/projects/golf_autoware/autoware/src/sensor_component/external/arena_camera/config/camera_f_info.yaml', 'r') as file:
             self.parameters = yaml.safe_load(file)


    def callback(self, pointcloud_msg, image_msg):
        points = []
        image_data=[]
        lidar_timestamp = float(pointcloud_msg.header.stamp.sec + pointcloud_msg.header.stamp.nanosec * 1e-9)
        for p in pc2.read_points(pointcloud_msg, field_names=("x", "y", "z"), skip_nans=True):
            points.append({
                "x": float(p[0]),
                "y": float(p[1]),
                "z": float(p[2])
            })
        self.json_data["points"] = points
        self.json_data["timestamp"] = lidar_timestamp
        image_data.append({
            "fx": self.parameters['camera_matrix']['data'][0],
            "timestamp": float(image_msg.header.stamp.sec + image_msg.header.stamp.nanosec * 1e-9),
            "p2": self.parameters['distortion_coefficients']['data'][3],
            "k1": self.parameters['distortion_coefficients']['data'][0],
            "p1": self.parameters['distortion_coefficients']['data'][2],
            "k3": self.parameters['distortion_coefficients']['data'][4],
            "k2": self.parameters['distortion_coefficients']['data'][1],
            "cy": self.parameters['camera_matrix']['data'][5],
            "cx": self.parameters['camera_matrix']['data'][2],
            "image_url": f"{self.msg_count}.png",
            "fy": self.parameters['camera_matrix']['data'][4],
            "position": {
                "y": -0.006,
                "x": 1.520,
                "z": 1.529
            },
            "heading": {
                "y": 0.497,
                "x":-0.500,
                "z":-0.496,
                "w":  0.506
            },
            "camera_model": "pinhole"
        })
        self.json_data["images"] = image_data
        json_filename = f"{self.msg_count}.json"

        json_path = os.path.join("/home/zeys/projects/deepenai2/camera_lidar_data/", json_filename)
        with open(json_path, "w") as json_file:
            json.dump(self.json_data, json_file, indent=4)



        # Convert image data to a NumPy array
        image_filename = f"{self.msg_count}.png"
        img_np = np.frombuffer(image_msg.data, dtype=np.uint8).reshape((image_msg.height, image_msg.width, -1))
        # Save image as PNG
        image_path = os.path.join("/home/zeys/projects/deepenai2/camera_lidar_data/", image_filename)
        self.msg_count += 1
        cv2.imwrite(image_path, img_np)
        self.get_logger().info(
            'Received synchronized messages: Image timestamp - {}, LaserScan timestamp - {}'
            .format(image_msg.header.stamp, pointcloud_msg.header.stamp)
        )



def main(args=None):
    rclpy.init(args=args)
    pointcloud_subscriber = PointcloudCamera()
    rclpy.spin(pointcloud_subscriber)
    rclpy.shutdown()


if __name__ == '__main__':
    main()