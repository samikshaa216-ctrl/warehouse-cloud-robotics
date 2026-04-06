"""
Battery-Aware Earliest Deadline First (BA-EDF) Scheduler
Combines:
- Deadline-driven task prioritization (EDF)
- Battery-aware robot selection
- Charging station management
- Task migration on robot failure
"""
import heapq
import time
import math
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

class TaskPriority(Enum):
    """Task priority levels"""
    LOW = 1
    MEDIUM = 2
    NORMAL = 3
    HIGH = 4
    CRITICAL = 5

class TaskStatus(Enum):
    """Task execution status"""
    PENDING = 'pending'
    ASSIGNED = 'assigned'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    TIMEOUT = 'timeout'

@dataclass
class Task:
    """Task representation"""
    task_id: str
    task_type: str  # PICKUP, DELIVER, PATROL, etc.
    deadline: float  # Unix timestamp
    estimated_duration: float  # Seconds
    priority: TaskPriority
    location: Tuple[float, float]  # (x, y)
    payload_weight: float = 0.0  # kg
    retry_count: int = 0
    status: TaskStatus = TaskStatus.PENDING
    assigned_robot: Optional[str] = None
    start_time: Optional[float] = None
    completion_time: Optional[float] = None
    
    def __lt__(self, other):
        """For heap comparison - earlier deadline = higher priority"""
        if self.deadline == other.deadline:
            return self.priority.value > other.priority.value
        return self.deadline < other.deadline
    
    def time_until_deadline(self) -> float:
        """Get time until deadline in seconds"""
        return max(0.0, self.deadline - time.time())
    
    def is_overdue(self) -> bool:
        """Check if task is past deadline"""
        return time.time() > self.deadline

@dataclass
class RobotCandidate:
    """Robot candidate for task assignment"""
    robot_id: str
    battery_level: float
    position: Tuple[float, float]
    status: str
    estimated_travel_time: float
    estimated_task_energy: float
    battery_margin: float  # Battery remaining after task
    total_score: float  # Combined selection score
    
    def __lt__(self, other):
        """For heap comparison - higher score is better"""
        return self.total_score > other.total_score

@dataclass
class ChargingStation:
    """Charging station representation"""
    station_id: str
    position: Tuple[float, float]
    occupied: bool = False
    occupied_by: Optional[str] = None
    queue: List[str] = field(default_factory=list)

class BatteryAwareScheduler:
    """
    Sophisticated scheduler combining multiple strategies:
    - EDF for deadline awareness
    - Battery prediction for feasibility
    - Energy-optimal robot selection
    - Charging queue management
    """
    
    # Configuration constants
    BATTERY_RESERVE = 15.0  # Minimum battery to maintain (%)
    BATTERY_CRITICAL = 20.0  # Force charging below this (%)
    BATTERY_LOW = 40.0  # Consider charging below this (%)
    MAX_TASK_RETRIES = 3  # Maximum retry attempts
    ROBOT_SPEED = 0.5  # m/s (for travel time estimation)
    
    # Scoring weights
    WEIGHT_BATTERY = 0.35
    WEIGHT_DISTANCE = 0.30
    WEIGHT_DEADLINE = 0.25
    WEIGHT_UTILIZATION = 0.10
    
    def __init__(self, charging_stations: List[ChargingStation]):
        """
        Initialize scheduler
        
        Args:
            charging_stations: List of available charging stations
        """
        self.task_queue: List[Task] = []
        self.robot_states: Dict[str, Dict[str, Any]] = {}
        self.charging_stations = {s.station_id: s for s in charging_stations}
        
        # Statistics
        self.stats = {
            'tasks_assigned': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'tasks_timeout': 0,
            'deadline_misses': 0,
            'charging_requests': 0,
            'total_assignment_time': 0.0
        }
    
    def add_task(self, task: Task):
        """
        Add task to queue (maintains EDF heap ordering)
        
        Args:
            task: Task to add
        """
        heapq.heappush(self.task_queue, task)
    
    def update_robot_state(self, robot_id: str, state: Dict[str, Any]):
        """
        Update stored robot state
        
        Args:
            robot_id: Robot identifier
            state: State dictionary with battery, position, status, etc.
        """
        self.robot_states[robot_id] = {
            **state,
            'last_update': time.time()
        }
    
    def assign_task(self) -> Optional[Tuple[str, Task]]:
        """
        Assign next task to best available robot using BA-EDF algorithm
        
        Returns:
            (robot_id, task) or None if no suitable assignment
        """
        if not self.task_queue:
            return None
        
        start_time = time.time()
        
        # Get highest priority task (earliest deadline)
        task = self.task_queue[0]
        
        # Check if task is overdue
        if task.is_overdue():
            heapq.heappop(self.task_queue)
            task.status = TaskStatus.TIMEOUT
            self.stats['tasks_timeout'] += 1
            self.stats['deadline_misses'] += 1
            return None
        
        # Find capable robots
        candidates = self._evaluate_robot_candidates(task)
        
        if not candidates:
            # No robot can complete task - keep in queue
            return None
        
        # Select best robot (highest total score)
        best_robot = max(candidates, key=lambda r: r.total_score)
        
        # Assign task
        heapq.heappop(self.task_queue)
        task.status = TaskStatus.ASSIGNED
        task.assigned_robot = best_robot.robot_id
        task.start_time = time.time()
        
        # Update statistics
        self.stats['tasks_assigned'] += 1
        assignment_time = time.time() - start_time
        self.stats['total_assignment_time'] += assignment_time
        
        return (best_robot.robot_id, task)
    
    def _evaluate_robot_candidates(self, task: Task) -> List[RobotCandidate]:
        """
        Evaluate all robots for task assignment
        
        Returns:
            List of viable robot candidates with scores
        """
        candidates = []
        
        for robot_id, state in self.robot_states.items():
            # Skip if not available
            if state.get('status') not in ['IDLE', 'AVAILABLE']:
                continue
            
            # Skip if charging or crashed
            if state.get('is_charging') or state.get('crashed'):
                continue
            
            # Skip if battery too low
            battery_level = state.get('battery_level', 0.0)
            if battery_level < self.BATTERY_CRITICAL:
                continue
            
            # Estimate travel time
            robot_pos = state.get('position', (0, 0))
            travel_time = self._estimate_travel_time(robot_pos, task.location)
            
            # Estimate energy consumption
            total_time = travel_time + task.estimated_duration
            task_energy = self._estimate_energy_consumption(
                total_time,
                task.task_type,
                task.payload_weight,
                state.get('battery_health', 100.0)
            )
            
            # Calculate battery margin
            battery_margin = battery_level - task_energy - self.BATTERY_RESERVE
            
            # Skip if insufficient battery
            if battery_margin < 0:
                continue
            
            # Calculate selection score
            total_score = self._calculate_robot_score(
                battery_level=battery_level,
                battery_margin=battery_margin,
                distance=self._calculate_distance(robot_pos, task.location),
                time_until_deadline=task.time_until_deadline(),
                robot_utilization=state.get('utilization', 0.0)
            )
            
            candidates.append(RobotCandidate(
                robot_id=robot_id,
                battery_level=battery_level,
                position=robot_pos,
                status=state.get('status', 'UNKNOWN'),
                estimated_travel_time=travel_time,
                estimated_task_energy=task_energy,
                battery_margin=battery_margin,
                total_score=total_score
            ))
        
        return candidates
    
    def _calculate_robot_score(
        self,
        battery_level: float,
        battery_margin: float,
        distance: float,
        time_until_deadline: float,
        robot_utilization: float
    ) -> float:
        """
        Calculate composite robot selection score
        
        Higher score = better candidate
        
        Factors:
        - Battery health (higher = better)
        - Distance to task (closer = better)
        - Deadline urgency (more time = better)
        - Robot utilization (lower = better for load balancing)
        """
        # Battery score (0-1, higher battery = higher score)
        battery_score = battery_level / 100.0
        
        # Distance score (0-1, closer = higher score)
        # Assuming max distance of 50m
        distance_score = max(0.0, 1.0 - (distance / 50.0))
        
        # Deadline urgency score (0-1, more time = higher score)
        # Normalize by task's estimated duration
        deadline_score = min(1.0, time_until_deadline / 300.0)  # 5 min baseline
        
        # Utilization score (0-1, lower utilization = higher score)
        utilization_score = 1.0 - robot_utilization
        
        # Weighted combination
        total_score = (
            self.WEIGHT_BATTERY * battery_score +
            self.WEIGHT_DISTANCE * distance_score +
            self.WEIGHT_DEADLINE * deadline_score +
            self.WEIGHT_UTILIZATION * utilization_score
        )
        
        return total_score
    
    def _estimate_travel_time(
        self,
        from_pos: Tuple[float, float],
        to_pos: Tuple[float, float]
    ) -> float:
        """
        Estimate travel time between two positions
        
        Returns: Time in seconds
        """
        distance = self._calculate_distance(from_pos, to_pos)
        return distance / self.ROBOT_SPEED
    
    def _calculate_distance(
        self,
        pos1: Tuple[float, float],
        pos2: Tuple[float, float]
    ) -> float:
        """Calculate Euclidean distance"""
        return math.sqrt((pos1[0] - pos2[0])**2 + (pos1[1] - pos2[1])**2)
    
    def _estimate_energy_consumption(
        self,
        duration: float,
        task_type: str,
        payload_weight: float,
        battery_health: float
    ) -> float:
        """
        Estimate battery percentage consumed for task
        
        Returns: Estimated battery percentage
        """
        # Base discharge rates (from battery model)
        DISCHARGE_IDLE = 0.5  # A
        DISCHARGE_MOVING = 2.0  # A
        DISCHARGE_CARRYING = 3.5  # A
        CAPACITY_AH = 50.0  # Ah
        
        # Estimate task profile
        if task_type in ['PICKUP', 'DELIVER']:
            # 70% moving, 50% carrying, 30% idle
            moving_time = duration * 0.7
            carrying_time = duration * 0.5
            idle_time = duration * 0.3
            
            total_discharge = (
                DISCHARGE_MOVING * moving_time +
                DISCHARGE_CARRYING * carrying_time +
                DISCHARGE_IDLE * idle_time
            ) / 3600.0  # Convert to Ah
        else:
            # Generic movement
            total_discharge = (DISCHARGE_MOVING * duration) / 3600.0
        
        # Adjust for payload (heavier = more energy)
        if payload_weight > 0:
            payload_factor = 1.0 + (payload_weight / 10.0) * 0.2  # 20% more per 10kg
            total_discharge *= payload_factor
        
        # Adjust for battery health
        health_factor = 1.0 + (100.0 - battery_health) / 100.0
        total_discharge *= health_factor
        
        # Convert to percentage
        return (total_discharge / CAPACITY_AH) * 100.0
    
    def should_charge(self, robot_id: str) -> bool:
        """
        Determine if robot should charge
        
        Args:
            robot_id: Robot identifier
            
        Returns:
            True if robot should charge
        """
        if robot_id not in self.robot_states:
            return False
        
        state = self.robot_states[robot_id]
        battery_level = state.get('battery_level', 100.0)
        
        # Force charging if critical
        if battery_level < self.BATTERY_CRITICAL:
            return True
        
        # Consider charging if low and no urgent tasks
        if battery_level < self.BATTERY_LOW:
            # Check if next task is far away deadline-wise
            if self.task_queue:
                next_task = self.task_queue[0]
                time_to_deadline = next_task.time_until_deadline()
                
                # If more than 5 minutes until deadline, charge
                if time_to_deadline > 300.0:
                    return True
        
        return False
    
    def assign_charging_station(
        self,
        robot_id: str,
        robot_position: Tuple[float, float]
    ) -> Optional[str]:
        """
        Assign robot to nearest available charging station
        
        Args:
            robot_id: Robot identifier
            robot_position: Robot's current position
            
        Returns:
            Charging station ID or None
        """
        # Find nearest available station
        best_station = None
        min_distance = float('inf')
        
        for station in self.charging_stations.values():
            if not station.occupied:
                distance = self._calculate_distance(robot_position, station.position)
                if distance < min_distance:
                    min_distance = distance
                    best_station = station
        
        if best_station:
            # Reserve station
            best_station.occupied = True
            best_station.occupied_by = robot_id
            self.stats['charging_requests'] += 1
            return best_station.station_id
        
        # All stations occupied - add to queue of nearest station
        nearest_station = min(
            self.charging_stations.values(),
            key=lambda s: self._calculate_distance(robot_position, s.position)
        )
        nearest_station.queue.append(robot_id)
        
        return nearest_station.station_id
    
    def release_charging_station(self, robot_id: str, station_id: str):
        """
        Release charging station and assign to next in queue
        
        Args:
            robot_id: Robot leaving station
            station_id: Station being released
        """
        if station_id not in self.charging_stations:
            return
        
        station = self.charging_stations[station_id]
        
        if station.occupied_by == robot_id:
            station.occupied = False
            station.occupied_by = None
            
            # Assign to next in queue
            if station.queue:
                next_robot = station.queue.pop(0)
                station.occupied = True
                station.occupied_by = next_robot
    
    def handle_robot_failure(self, robot_id: str):
        """
        Handle robot failure - reassign its task
        
        Args:
            robot_id: Failed robot identifier
        """
        # Find tasks assigned to this robot
        for task in self.task_queue:
            if task.assigned_robot == robot_id:
                task.retry_count += 1
                
                if task.retry_count >= self.MAX_TASK_RETRIES:
                    task.status = TaskStatus.FAILED
                    self.stats['tasks_failed'] += 1
                else:
                    # Reset for reassignment
                    task.status = TaskStatus.PENDING
                    task.assigned_robot = None
                    task.start_time = None
                    heapq.heapify(self.task_queue)  # Re-heapify
    
    def complete_task(self, task_id: str):
        """Mark task as completed"""
        # Find task in queue
        for task in self.task_queue:
            if task.task_id == task_id:
                task.status = TaskStatus.COMPLETED
                task.completion_time = time.time()
                
                # Check if deadline was met
                if task.completion_time and task.deadline:
                    if task.completion_time > task.deadline:
                        self.stats['deadline_misses'] += 1
                
                self.stats['tasks_completed'] += 1
                break
    
    def get_pending_tasks_count(self) -> int:
        """Get number of pending tasks"""
        return len([t for t in self.task_queue if t.status == TaskStatus.PENDING])
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        avg_assignment_time = 0.0
        if self.stats['tasks_assigned'] > 0:
            avg_assignment_time = (self.stats['total_assignment_time'] / 
                                  self.stats['tasks_assigned'])
        
        deadline_miss_rate = 0.0
        if self.stats['tasks_completed'] > 0:
            deadline_miss_rate = (self.stats['deadline_misses'] / 
                                 self.stats['tasks_completed']) * 100.0
        
        return {
            **self.stats,
            'avg_assignment_time_ms': avg_assignment_time * 1000.0,
            'deadline_miss_rate_pct': deadline_miss_rate,
            'pending_tasks': self.get_pending_tasks_count()
        }

# Example usage
if __name__ == '__main__':
    # Create charging stations
    charging_stations = [
        ChargingStation('charger_1', (5, 5)),
        ChargingStation('charger_2', (45, 5)),
        ChargingStation('charger_3', (25, 25))
    ]
    
    # Initialize scheduler
    scheduler = BatteryAwareScheduler(charging_stations)
    
    # Add some robots
    for i in range(5):
        scheduler.update_robot_state(f'robot_{i}', {
            'battery_level': 80.0 - (i * 10),
            'position': (i * 10.0, i * 5.0),
            'status': 'IDLE',
            'battery_health': 100.0,
            'utilization': 0.0
        })
    
    # Add some tasks
    current_time = time.time()
    tasks = [
        Task('task_1', 'PICKUP', current_time + 300, 60.0, TaskPriority.HIGH, (20, 15)),
        Task('task_2', 'DELIVER', current_time + 600, 90.0, TaskPriority.NORMAL, (35, 20)),
        Task('task_3', 'PATROL', current_time + 180, 45.0, TaskPriority.CRITICAL, (10, 10))
    ]
    
    for task in tasks:
        scheduler.add_task(task)
    
    print("Battery-Aware Scheduler Test")
    print("=" * 50)
    
    # Assign tasks
    for _ in range(len(tasks)):
        assignment = scheduler.assign_task()
        if assignment:
            robot_id, task = assignment
            print(f"\nAssigned Task {task.task_id} to {robot_id}")
            print(f"  Priority: {task.priority.name}")
            print(f"  Deadline: {task.time_until_deadline():.0f}s away")
        else:
            print("\nNo suitable robot for next task")
    
    print("\n" + "=" * 50)
    print("Scheduler Statistics:")
    stats = scheduler.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")