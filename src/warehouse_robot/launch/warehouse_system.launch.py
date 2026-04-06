"""
Dynamic Warehouse System Launch File
Supports configurable number of robots (1-10)
"""
import os
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction, LogInfo
from launch.substitutions import LaunchConfiguration, TextSubstitution
from launch_ros.actions import Node

def generate_robot_nodes(context, *args, **kwargs):
    """Generate robot nodes dynamically based on num_robots parameter"""
    num_robots = int(LaunchConfiguration('num_robots').perform(context))
    
    if num_robots < 1 or num_robots > 10:
        raise ValueError(f"num_robots must be between 1 and 10, got {num_robots}")
    
    # Calculate robot starting positions in grid
    grid_width = 50
    grid_height = 30
    
    robots_per_row = min(5, num_robots)
    spacing_x = grid_width / (robots_per_row + 1)
    spacing_y = grid_height / ((num_robots // robots_per_row) + 2)
    
    nodes = []
    
    # Create robot nodes
    for i in range(num_robots):
        robot_id = f"robot_{i+1:03d}"
        
        # Calculate position
        row = i // robots_per_row
        col = i % robots_per_row
        start_x = spacing_x * (col + 1)
        start_y = spacing_y * (row + 1)
        
        robot_node = Node(
            package='warehouse_robot',
            executable='robot_core',
            name=f'robot_core_{i+1}',
            namespace=robot_id,
            parameters=[{
                'robot_id': robot_id,
                'start_position_x': start_x,
                'start_position_y': start_y,
                'enable_battery_sim': True,
                'enable_checkpointing': True
            }],
            output='screen'
        )
        nodes.append(robot_node)
        
        nodes.append(LogInfo(
            msg=f"Starting {robot_id} at ({start_x:.1f}, {start_y:.1f})"
        ))
    
    return nodes

def generate_launch_description():
    """Generate complete launch description"""
    
    # Declare arguments
    num_robots_arg = DeclareLaunchArgument(
        'num_robots',
        default_value='6',
        description='Number of robots to spawn (1-10)'
    )
    
    # Central allocator node
    allocator_node = Node(
        package='warehouse_robot',
        executable='allocator_node',
        name='allocator_node',
        output='screen'
    )
    
    # Warehouse graph node
    warehouse_graph_node = Node(
        package='warehouse_robot',
        executable='warehouse_graph',
        name='warehouse_graph',
        output='screen'
    )
    
    # Dashboard server node
    dashboard_node = Node(
        package='warehouse_robot',
        executable='dashboard_server',
        name='dashboard_server',
        parameters=[{'port': 8080}],
        output='screen'
    )
    
    return LaunchDescription([
        num_robots_arg,
        LogInfo(msg="Starting Warehouse Cloud Robotics System"),
        LogInfo(msg=["Number of robots: ", LaunchConfiguration('num_robots')]),
        allocator_node,
        warehouse_graph_node,
        dashboard_node,
        OpaqueFunction(function=generate_robot_nodes),
        LogInfo(msg="All nodes started successfully")
    ])