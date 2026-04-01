from setuptools import find_packages, setup

package_name = 'warehouse_robot'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='samiksha',
    maintainer_email='samiksha@todo.todo',
    description='Distributed warehouse robot simulation',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'robot_core = warehouse_robot.robot_core:main',
            'allocator_node = warehouse_robot.allocator_node:main',
            'dashboard_server = warehouse_robot.dashboard_server:main',
        ],
    },
)
