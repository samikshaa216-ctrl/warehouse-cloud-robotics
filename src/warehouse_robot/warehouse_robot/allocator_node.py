import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import random
import time
import csv
import os


class AllocatorNode(Node):

    def __init__(self):
        super().__init__('allocator_node')

        self.robots          = {}
        self.task_counter    = 0
        self.active_tasks    = {}
        self.task_targets    = {}
        self.task_deadlines  = {}
        self.task_created_at = {}
        self.task_publishers = {}
        self.task_robot_map  = {}

        # EDF stats
        self.total_tasks      = 0
        self.deadlines_met    = 0
        self.deadlines_missed = 0

        # Timing
        self.step_interval  = 0.5
        self.deadline_slack = 3.5

        # CSV setup
        self.csv_path = os.path.expanduser('~/warehouse_ws/experiment_results.csv')
        self._init_csv()

        self.experiment_start = time.time()

        # Dashboard publishers
        self.assignment_pub = self.create_publisher(String, 'task_assignment', 10)
        self.log_pub        = self.create_publisher(String, 'dashboard_log', 10)

        self.create_subscription(String, 'robot_registration', self.register_robot, 10)
        self.create_subscription(String, 'robot_heartbeat',    self.heartbeat_callback, 10)
        self.create_subscription(String, 'task_completion',    self.task_completion_callback, 10)

        self.create_timer(1.0, self.monitor_robots)
        self.create_timer(5.0, self.print_stats)

        self.get_logger().info("Allocator Node Started — EDF Scheduling + CSV Logging Active")
        self.get_logger().info(f"Logging to: {self.csv_path}")

    # ---------------- CSV INIT ----------------
    def _init_csv(self):
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'task_id',
                'robot_id',
                'goal_x',
                'goal_y',
                'manhattan_dist',
                'battery_at_assign',
                'battery_at_complete',
                'created_at',
                'completed_at',
                'duration_s',
                'deadline_s',
                'deadline_met',
                'outcome',
                'experiment_time_s'
            ])
        self.get_logger().info("CSV initialized")

    # ---------------- CSV WRITE ROW ----------------
    def _write_csv_row(self, task_id, robot_id, outcome, battery_complete, met):
        goal = self.task_targets.get(task_id)
        if goal is None:
            return

        goal_x, goal_y   = goal
        created_at        = self.task_created_at.get(task_id, 0)
        deadline          = self.task_deadlines.get(task_id, 0)
        completed_at      = time.time()
        duration          = round(completed_at - created_at, 3) if created_at else 0
        deadline_s        = round(deadline - created_at, 3) if created_at and deadline else 0
        exp_time          = round(completed_at - self.experiment_start, 3)
        battery_assign    = self.task_robot_map.get(f"{task_id}_battery", 0)
        dist              = self.task_robot_map.get(f"{task_id}_dist", 0)
        robot_id_log      = self.task_robot_map.get(task_id, robot_id)

        with open(self.csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                task_id,
                robot_id_log,
                goal_x,
                goal_y,
                dist,
                round(battery_assign, 1),
                round(battery_complete, 1),
                round(created_at, 3),
                round(completed_at, 3),
                duration,
                deadline_s,
                1 if met else 0,
                outcome,
                exp_time
            ])

    # ---------------- REGISTER ROBOT ----------------
    def register_robot(self, msg):
        robot_id, x, y = msg.data.split(",")
        self.robots[robot_id] = {
            "x":               int(x),
            "y":               int(y),
            "last_heartbeat":  time.time(),
            "status":          "IDLE",
            "current_task":    None,
            "battery":         100.0,
            "tasks_completed": 0,
            "crash_handled":   False
        }
        self.get_logger().info(f"{robot_id} registered at ({x},{y})")
        self.assign_task(robot_id)

    # ---------------- PUBLISHER CACHE ----------------
    def get_task_publisher(self, robot_id):
        if robot_id not in self.task_publishers:
            self.task_publishers[robot_id] = self.create_publisher(
                String, f'/{robot_id}/task', 10
            )
        return self.task_publishers[robot_id]

    # ---------------- HELPERS ----------------
    def manhattan(self, x1, y1, x2, y2):
        return abs(x2 - x1) + abs(y2 - y1)

    def battery_needed(self, robot_id, goal_x, goal_y):
        rx = self.robots[robot_id]["x"]
        ry = self.robots[robot_id]["y"]
        dist = self.manhattan(rx, ry, goal_x, goal_y)
        return (dist * 2.0) + 20.0

    def compute_deadline(self, robot_id, goal_x, goal_y):
        rx = self.robots[robot_id]["x"]
        ry = self.robots[robot_id]["y"]
        dist = self.manhattan(rx, ry, goal_x, goal_y)
        time_allowed = dist * self.step_interval * self.deadline_slack
        return time.time() + max(time_allowed, 5.0)

    # ---------------- ASSIGN TASK ----------------
    def assign_task(self, robot_id):
        if robot_id not in self.robots:
            return
        if self.robots[robot_id]["current_task"] is not None:
            return
        if self.robots[robot_id]["status"] not in ("IDLE",):
            return

        battery = self.robots[robot_id]["battery"]
        if battery < 30.0:
            self.get_logger().warning(
                f"{robot_id} battery too low ({battery:.1f}%) — waiting for recharge"
            )
            return

        self.task_counter += 1
        task_id = f"T{self.task_counter}"

        attempts = 0
        while attempts < 10:
            goal_x = random.randint(0, 29)
            goal_y = random.randint(0, 29)
            if battery >= self.battery_needed(robot_id, goal_x, goal_y):
                break
            attempts += 1

        if attempts == 10:
            rx = self.robots[robot_id]["x"]
            ry = self.robots[robot_id]["y"]
            max_steps = max(1, int((battery - 20.0) / 2.0))
            goal_x = max(0, min(29, rx + random.randint(-max_steps, max_steps)))
            goal_y = max(0, min(29, ry + random.randint(-max_steps, max_steps)))

        deadline    = self.compute_deadline(robot_id, goal_x, goal_y)
        deadline_in = deadline - time.time()

        rx   = self.robots[robot_id]["x"]
        ry   = self.robots[robot_id]["y"]
        dist = self.manhattan(rx, ry, goal_x, goal_y)

        # Snapshot for CSV
        self.task_robot_map[task_id]              = robot_id
        self.task_robot_map[f"{task_id}_battery"] = battery
        self.task_robot_map[f"{task_id}_dist"]    = dist

        self.active_tasks[task_id]            = robot_id
        self.task_targets[task_id]            = (goal_x, goal_y)
        self.task_deadlines[task_id]          = deadline
        self.task_created_at[task_id]         = time.time()
        self.robots[robot_id]["current_task"] = task_id
        self.robots[robot_id]["status"]       = "ACTIVE"
        self.total_tasks += 1

        pub = self.get_task_publisher(robot_id)
        msg = String()
        msg.data = f"{task_id},{rx},{ry},{goal_x},{goal_y}"
        pub.publish(msg)

        # Broadcast to dashboard
        dash_msg = String()
        dash_msg.data = f"{task_id},{robot_id},{goal_x},{goal_y},{deadline_in:.1f}"
        self.assignment_pub.publish(dash_msg)

        self.get_logger().info(
            f"[EDF] Assigned {task_id} → {robot_id} "
            f"| goal=({goal_x},{goal_y}) dist={dist} "
            f"| battery={battery:.1f}% "
            f"| deadline_in={deadline_in:.1f}s"
        )

    # ---------------- HEARTBEAT ----------------
    def heartbeat_callback(self, msg):
        parts = msg.data.split(",")
        if len(parts) < 4:
            return

        robot_id = parts[0]
        x        = parts[1]
        y        = parts[2]
        status   = parts[3]
        battery  = float(parts[4]) if len(parts) > 4 else None

        if robot_id not in self.robots:
            return

        self.robots[robot_id]["x"]              = int(x)
        self.robots[robot_id]["y"]              = int(y)
        self.robots[robot_id]["last_heartbeat"] = time.time()
        self.robots[robot_id]["status"]         = status

        if battery is not None:
            self.robots[robot_id]["battery"] = battery

        if status in ("IDLE", "CHARGING"):
            if self.robots[robot_id].get("crash_handled"):
                self.robots[robot_id]["crash_handled"] = False

        if status == "CRASHED":
            if self.robots[robot_id].get("crash_handled"):
                return
            self.robots[robot_id]["crash_handled"] = True
            self.get_logger().warning(f"{robot_id} reported CRASH")
            failed_task = self.robots[robot_id]["current_task"]
            if failed_task:
                battery_now = self.robots[robot_id]["battery"]
                self._record_deadline(failed_task, met=False, reason="CRASH")
                self._write_csv_row(failed_task, robot_id, "CRASHED", battery_now, False)
                self.robots[robot_id]["current_task"] = None
                self.robots[robot_id]["status"]       = "IDLE"
                self.reassign_task(failed_task, robot_id)

    # ---------------- TASK COMPLETE ----------------
    def task_completion_callback(self, msg):
        robot_id, task_id, result = msg.data.split(",")

        if task_id in self.active_tasks:
            del self.active_tasks[task_id]

        if robot_id in self.robots:
            self.robots[robot_id]["current_task"]    = None
            self.robots[robot_id]["status"]          = "IDLE"
            self.robots[robot_id]["tasks_completed"] += 1

        battery = self.robots.get(robot_id, {}).get("battery", 0)
        total   = self.robots.get(robot_id, {}).get("tasks_completed", 0)

        met = self._record_deadline(task_id, met=None, reason="COMPLETE")
        self._write_csv_row(task_id, robot_id, "COMPLETED", battery, met)

        deadline_str = "✅ ON TIME" if met else "❌ LATE"

        self.get_logger().info(
            f"{robot_id} completed {task_id} {deadline_str} "
            f"| battery={battery:.1f}% | total={total} "
            f"| met={self.deadlines_met} missed={self.deadlines_missed}"
        )

        # Push to dashboard log
        log_msg = String()
        log_msg.data = f"{robot_id} {deadline_str} {task_id} | battery={battery:.1f}%"
        self.log_pub.publish(log_msg)

        self.assign_task(robot_id)

    # ---------------- RECORD DEADLINE ----------------
    def _record_deadline(self, task_id, met, reason):
        if task_id not in self.task_deadlines:
            return True

        deadline = self.task_deadlines[task_id]
        now      = time.time()

        if met is None:
            met = (now <= deadline)

        if met:
            self.deadlines_met += 1
        else:
            self.deadlines_missed += 1
            remaining = deadline - now
            self.get_logger().warning(
                f"[EDF] DEADLINE MISSED for {task_id} "
                f"({reason}) | overdue by {-remaining:.1f}s"
            )

        self.task_deadlines.pop(task_id, None)
        self.task_created_at.pop(task_id, None)

        return met

    # ---------------- MONITOR ROBOTS ----------------
    def monitor_robots(self):
        current_time = time.time()

        for robot_id, data in list(self.robots.items()):
            time_since_hb = current_time - data["last_heartbeat"]

            if time_since_hb > 8 and data["status"] == "ACTIVE":
                self.get_logger().warning(
                    f"{robot_id} TIMEOUT ({time_since_hb:.1f}s)"
                )
                failed_task = data["current_task"]
                if failed_task:
                    battery_now = data["battery"]
                    self._record_deadline(failed_task, met=False, reason="TIMEOUT")
                    self._write_csv_row(failed_task, robot_id, "TIMEOUT", battery_now, False)
                    self.robots[robot_id]["current_task"] = None
                    self.robots[robot_id]["status"]       = "IDLE"
                    self.reassign_task(failed_task, robot_id)

            task_id = data.get("current_task")
            if task_id and task_id in self.task_deadlines:
                remaining = self.task_deadlines[task_id] - current_time
                if 0 < remaining < 3.0:
                    self.get_logger().warning(
                        f"[EDF] ⚠️  {robot_id} task {task_id} deadline in {remaining:.1f}s!"
                    )

            if (data["status"] == "IDLE"
                    and data["current_task"] is None
                    and data["battery"] >= 30.0
                    and not data.get("crash_handled")):
                self.assign_task(robot_id)

    # ---------------- REASSIGN TASK ----------------
    def reassign_task(self, task_id, failed_robot):
        if task_id not in self.task_targets:
            return

        goal_x, goal_y = self.task_targets[task_id]
        best_robot      = None
        best_battery    = -1

        for robot_id, data in self.robots.items():
            if robot_id == failed_robot:
                continue
            if data["status"] == "IDLE" and data["battery"] >= 30.0:
                needed = self.battery_needed(robot_id, goal_x, goal_y)
                if data["battery"] >= needed and data["battery"] > best_battery:
                    best_battery = data["battery"]
                    best_robot   = robot_id

        if best_robot is None:
            self.get_logger().warning(f"No suitable robot to reassign {task_id}")
            self._write_csv_row(task_id, failed_robot, "REASSIGN_FAILED", 0.0, False)
            self.task_targets.pop(task_id, None)
            return

        existing_deadline = self.task_deadlines.get(task_id)
        if existing_deadline is None:
            existing_deadline = self.compute_deadline(best_robot, goal_x, goal_y)
            self.task_deadlines[task_id] = existing_deadline

        time_left = existing_deadline - time.time()

        pub = self.get_task_publisher(best_robot)
        msg = String()
        msg.data = (
            f"{task_id},"
            f"{self.robots[best_robot]['x']},"
            f"{self.robots[best_robot]['y']},"
            f"{goal_x},{goal_y}"
        )
        pub.publish(msg)

        self.active_tasks[task_id]              = best_robot
        self.robots[best_robot]["current_task"] = task_id
        self.robots[best_robot]["status"]       = "ACTIVE"

        self.get_logger().info(
            f"[EDF] Reassigned {task_id} → {best_robot} "
            f"| battery={best_battery:.1f}% "
            f"| deadline_remaining={time_left:.1f}s"
        )

    # ---------------- PRINT STATS ----------------
    def print_stats(self):
        if self.total_tasks == 0:
            return
        rate     = (self.deadlines_met / self.total_tasks) * 100.0
        exp_time = round(time.time() - self.experiment_start, 1)
        self.get_logger().info(
            f"━━━ EDF STATS [{exp_time}s] ━━━ "
            f"total={self.total_tasks} "
            f"met={self.deadlines_met} "
            f"missed={self.deadlines_missed} "
            f"success_rate={rate:.1f}%"
        )
        # Push stats to dashboard log
        log_msg = String()
        log_msg.data = (
            f"[STATS] total={self.total_tasks} "
            f"met={self.deadlines_met} "
            f"missed={self.deadlines_missed} "
            f"rate={rate:.1f}%"
        )
        self.log_pub.publish(log_msg)


def main(args=None):
    rclpy.init(args=args)
    node = AllocatorNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
