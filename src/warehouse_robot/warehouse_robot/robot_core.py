import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import random
import time
import json
import os


class WarehouseRobot(Node):

    def __init__(self):
        super().__init__('warehouse_robot')

        # PARAMETERS
        self.declare_parameter("robot_id", "robot_1")
        self.declare_parameter("start_x", 0.0)
        self.declare_parameter("start_y", 0.0)
        self.declare_parameter("crash_mode", "none")
        self.declare_parameter("crash_percentage", 50.0)
        self.declare_parameter("failure_probability", 0.05)
        self.declare_parameter("recovery_timeout", 5.0)
        self.declare_parameter("step_interval", 0.5)
        self.declare_parameter("battery_level", 100.0)
        self.declare_parameter("battery_drain_per_step", 2.0)
        self.declare_parameter("battery_critical_threshold", 20.0)
        self.declare_parameter("battery_recharge_rate", 1.0)

        self.robot_id              = self.get_parameter("robot_id").value
        self.x                     = int(self.get_parameter("start_x").value)
        self.y                     = int(self.get_parameter("start_y").value)
        self.crash_mode            = self.get_parameter("crash_mode").value
        self.crash_percentage      = float(self.get_parameter("crash_percentage").value)
        self.failure_probability   = float(self.get_parameter("failure_probability").value)
        self.recovery_timeout      = float(self.get_parameter("recovery_timeout").value)
        self.step_interval         = float(self.get_parameter("step_interval").value)
        self.battery               = float(self.get_parameter("battery_level").value)
        self.battery_drain         = float(self.get_parameter("battery_drain_per_step").value)
        self.battery_critical      = float(self.get_parameter("battery_critical_threshold").value)
        self.battery_recharge_rate = float(self.get_parameter("battery_recharge_rate").value)

        # STATE
        self.current_path                  = []
        self.current_step                  = 0
        self.task_id                       = None
        self.task_active                   = False
        self.crashed                       = False
        self.recovery_timer                = None
        self.deterministic_crash_triggered = False
        self.battery_depleted              = False

        # CHECKPOINT STATE
        self.checkpoint_dir        = os.path.expanduser(f'~/warehouse_ws/checkpoints')
        self.steps_since_checkpoint = 0
        self.total_checkpoints      = 0
        self.checkpoint_interval    = 10     # default: every 10 steps
        self.task_deadline          = None   # set when task is received

        os.makedirs(self.checkpoint_dir, exist_ok=True)

        # PUBLISHERS
        self.registration_pub = self.create_publisher(String, 'robot_registration', 10)
        self.heartbeat_pub    = self.create_publisher(String, 'robot_heartbeat', 10)
        self.completion_pub   = self.create_publisher(String, 'task_completion', 10)

        # TASK SUBSCRIPTION
        self.task_sub = self.create_subscription(
            String,
            f'/{self.robot_id}/task',
            self.task_callback,
            10
        )

        self.timer = self.create_timer(self.step_interval, self.update_robot)

        self.get_logger().info(
            f"{self.robot_id} started at ({self.x},{self.y}) "
            f"| CrashMode={self.crash_mode} | Battery={self.battery}%"
        )

        self.register_robot()

    # ---------------- REGISTER ----------------
    def register_robot(self):
        msg = String()
        msg.data = f"{self.robot_id},{self.x},{self.y}"
        self.registration_pub.publish(msg)

    # ---------------- TASK RECEIVED ----------------
    def task_callback(self, msg):
        if self.battery_depleted or self.crashed:
            self.get_logger().warning(
                f"{self.robot_id} ignoring task — "
                f"{'depleted' if self.battery_depleted else 'crashed'}"
            )
            return

        data   = msg.data.split(",")
        self.task_id   = data[0]
        goal_x = int(data[3])
        goal_y = int(data[4])

        self.current_path                  = self.generate_path(self.x, self.y, goal_x, goal_y)
        self.current_step                  = 0
        self.task_active                   = True
        self.deterministic_crash_triggered = False
        self.steps_since_checkpoint        = 0

        # Estimate deadline: path_length * step_interval * 3.5 slack
        path_len          = len(self.current_path)
        self.task_deadline = time.time() + (path_len * self.step_interval * 3.5)

        self.get_logger().info(
            f"{self.robot_id} received Task {self.task_id} "
            f"| goal=({goal_x},{goal_y}) | battery={self.battery:.1f}% "
            f"| path_len={path_len}"
        )

        # Save initial checkpoint on task receipt
        self._save_checkpoint("TASK_START")

    # ---------------- PATH GENERATION ----------------
    def generate_path(self, sx, sy, gx, gy):
        path = []
        x, y = sx, sy
        while x != gx:
            x += 1 if gx > x else -1
            path.append((x, y))
        while y != gy:
            y += 1 if gy > y else -1
            path.append((x, y))
        return path

    # ---------------- HEARTBEAT ----------------
    def send_heartbeat(self, status):
        msg      = String()
        msg.data = f"{self.robot_id},{self.x},{self.y},{status},{self.battery:.1f}"
        self.heartbeat_pub.publish(msg)

    # ---------------- CHECKPOINT LOGIC ----------------
    def _compute_checkpoint_interval(self):
        """
        Adaptive checkpoint frequency:
          NORMAL:           every 10 steps
          battery_low:      every 5  steps
          deadline_close:   every 5  steps
          both critical:    every 1  step  (checkpoint every move)
        """
        battery_low     = self.battery < self.battery_critical       # below 20%
        deadline_close  = False

        if self.task_deadline is not None:
            time_remaining = self.task_deadline - time.time()
            steps_left     = len(self.current_path) - self.current_step
            time_needed    = steps_left * self.step_interval
            # Deadline is close if we have less than 30% time buffer left
            deadline_close = (time_remaining < time_needed * 1.3)

        if battery_low and deadline_close:
            return 1    # checkpoint every step
        elif battery_low or deadline_close:
            return 5    # checkpoint every 5 steps
        else:
            return 10   # checkpoint every 10 steps

    def _save_checkpoint(self, trigger="PERIODIC"):
        """Save robot state to a JSON file simulating cloud checkpoint."""
        self.total_checkpoints += 1
        interval = self._compute_checkpoint_interval()

        checkpoint = {
            "robot_id":        self.robot_id,
            "task_id":         self.task_id,
            "x":               self.x,
            "y":               self.y,
            "battery":         round(self.battery, 1),
            "current_step":    self.current_step,
            "path_remaining":  len(self.current_path) - self.current_step,
            "timestamp":       round(time.time(), 3),
            "trigger":         trigger,
            "checkpoint_interval": interval,
            "total_checkpoints":   self.total_checkpoints
        }

        path = os.path.join(
            self.checkpoint_dir,
            f"{self.robot_id}_latest.json"
        )
        with open(path, 'w') as f:
            json.dump(checkpoint, f, indent=2)

        self.get_logger().info(
            f"[CKPT] {self.robot_id} checkpoint #{self.total_checkpoints} "
            f"| trigger={trigger} "
            f"| interval={interval} "
            f"| battery={self.battery:.1f}% "
            f"| step={self.current_step}/{len(self.current_path)}"
        )

        self.steps_since_checkpoint = 0

    def _maybe_checkpoint(self):
        """Called after every step — checkpoints if interval is reached."""
        self.steps_since_checkpoint += 1
        interval = self._compute_checkpoint_interval()
        self.checkpoint_interval = interval

        if self.steps_since_checkpoint >= interval:
            self._save_checkpoint("PERIODIC")

    # ---------------- CRASH ----------------
    def simulate_crash(self, reason):
        if self.crashed:
            return
        self.crashed     = True
        self.task_active = False
        self.get_logger().error(f"{self.robot_id} {reason}")
        # Checkpoint on crash so state can be recovered
        self._save_checkpoint("PRE_CRASH")
        self.send_heartbeat("CRASHED")
        if self.recovery_timer is None:
            self.recovery_timer = self.create_timer(
                self.recovery_timeout,
                self.recover_from_crash
            )

    def recover_from_crash(self):
        if not self.crashed:
            return
        self.get_logger().info(
            f"{self.robot_id} RECOVERED after {self.recovery_timeout}s "
            f"| battery={self.battery:.1f}% — now recharging"
        )
        self.crashed     = False
        self.task_active = False
        if self.recovery_timer:
            self.recovery_timer.cancel()
            self.recovery_timer = None

    # ---------------- MAIN LOOP ----------------
    def update_robot(self):

        if self.crashed:
            self.battery = min(100.0, self.battery + self.battery_recharge_rate)
            return

        # CHARGING STATE
        if self.battery_depleted:
            self.battery = min(100.0, self.battery + self.battery_recharge_rate * 2.0)
            self.send_heartbeat("CHARGING")
            self.get_logger().info(
                f"{self.robot_id} CHARGING | battery={self.battery:.1f}%"
            )
            if self.battery >= 80.0:
                self.battery_depleted = False
                self.get_logger().info(
                    f"{self.robot_id} fully recharged to {self.battery:.1f}% "
                    f"— ready for new task"
                )
                self.send_heartbeat("IDLE")
            return

        # BATTERY DEPLETED CHECK
        if self.battery <= 0.0:
            self.battery          = 0.0
            self.battery_depleted = True
            self.task_active      = False
            self.get_logger().error(
                f"{self.robot_id} BATTERY DEPLETED — going to charging dock"
            )
            self._save_checkpoint("BATTERY_DEPLETED")
            self.send_heartbeat("CHARGING")
            return

        # IDLE — recharge slowly
        if not self.task_active:
            self.battery = min(100.0, self.battery + self.battery_recharge_rate)
            self.send_heartbeat("IDLE")
            return

        # TASK COMPLETE
        if self.current_step >= len(self.current_path):
            self.get_logger().info(
                f"{self.robot_id} completed Task {self.task_id} "
                f"| battery={self.battery:.1f}% "
                f"| total_checkpoints={self.total_checkpoints}"
            )
            self._save_checkpoint("TASK_COMPLETE")
            msg      = String()
            msg.data = f"{self.robot_id},{self.task_id},COMPLETED"
            self.completion_pub.publish(msg)
            self.task_active   = False
            self.task_deadline = None
            self.send_heartbeat("IDLE")
            return

        # MOVE ONE STEP
        self.x, self.y = self.current_path[self.current_step]
        self.current_step += 1

        drain = self.battery_drain
        if self.battery < self.battery_critical:
            drain = self.battery_drain * 1.5
        self.battery = max(0.0, self.battery - drain)

        self.get_logger().info(
            f"{self.robot_id} moved to ({self.x},{self.y}) "
            f"| battery={self.battery:.1f}% "
            f"| ckpt_interval={self._compute_checkpoint_interval()}"
        )

        self.send_heartbeat("ACTIVE")

        if self.battery < self.battery_critical:
            self.get_logger().warning(
                f"{self.robot_id} LOW BATTERY: {self.battery:.1f}%"
            )

        # Adaptive checkpoint check
        self._maybe_checkpoint()

        # CRASH MODES
        if self.crash_mode == "deterministic" and not self.deterministic_crash_triggered:
            progress = (self.current_step / len(self.current_path)) * 100.0
            if progress >= self.crash_percentage:
                self.deterministic_crash_triggered = True
                self.simulate_crash(
                    f"SIMULATED DETERMINISTIC CRASH at {progress:.2f}%"
                )
                return

        if self.crash_mode == "random":
            if random.random() < self.failure_probability:
                self.simulate_crash("SIMULATED RANDOM CRASH")
                return


def main(args=None):
    rclpy.init(args=args)
    node = WarehouseRobot()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
