FROM ros:humble

WORKDIR /warehouse_ws
COPY . .

SHELL ["bash", "-c"]

RUN source /opt/ros/humble/setup.bash && \
    colcon build

CMD ["bash", "-c", "source /opt/ros/humble/setup.bash && source install/setup.bash && ros2 run warehouse_robot allocator_node"]
