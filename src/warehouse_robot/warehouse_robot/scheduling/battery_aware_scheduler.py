"""
Battery-Aware EDF (Earliest Deadline First) Scheduler
Combines deadline-driven priority with battery awareness
"""
import heapq
import time
import math
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum

class TaskPriority(Enum):
    """Task priority levels"""
    LOW = 1
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

@dataclass
class Task:
    """Task representation"""
    task_id: str
    task_type: str  # PICKUP, DELIVER, PATROL
    deadline: float  # Unix timestamp
    estimated_duration: float  # Seconds
    priority: TaskPriority
    location: Tuple[float, float]  # (x, y)
    status: TaskStatus = TaskStatus.PENDING
    assigned_robot: Optional[str] = None
    retry_count: int = 0
    
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
    battery_margin: float
    total_score: float
    
    def __lt__(self, other):
        """Higher score is better"""
        return self.total_score > other.total_score

@dataclass
class ChargingStation:
    """Charging station representation"""
    station_id: str
    position: Tuple[float, float]
    occupied: bool = False
    occupied_by: Optional[str] = None

class BatteryAwareScheduler:
    """
    Intelligent scheduler combining:
    - EDF for deadline awareness
    - Battery prediction for feasibility
    - Energy-optimal robot selection
    """
    
    # Configuration
    BATTERY_RESERVE = 15.0  # Minimum battery to maintain (%)
    BATTERY_CRITICAL = 20.0  # Force charging below this (%)
    MAX_TASK_RETRIES = 3
    ROBOT_SPEED = 0.5  # m/s
    
    # Scoring weights
    WEIGHT_BATTERY = 0.40
    WEIGHT_DISTANCE = 0.35
    WEIGHT_DEADLINE = 0.25
    
    def __init__(self, charging_stations: List[ChargingStation]):
        self.task_queue: List[Task] = []
        self.robot_states: Dict[str, Dict[str, Any]] = {}
        self.charging_stations = {s.station_id: s for s in charging_stations}
        
        # Statistics
        self.stats = {
            'tasks_assigned': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'deadline_misses': 0
        }
    
    def add_task(self, task: Task):
        """Add task to queue (maintains EDF heap ordering)"""
        heapq.heappush(self.task_queue, task)
    
    def update_robot_state(self, robot_id: str, state: Dict[str, Any]):
        """Update stored robot state"""
        self.robot_states[robot_id] = {
            **state,
            'last_update': time.time()
        }
    
    def assign_task(self) -> Optional[Tuple[str, Task]]:
        """
        Assign next task to best available robot
        
        Returns:
            (robot_id, task) or None if no suitable assignment
        """
        if not self.task_queue:
            return None
        
        # Get highest priority task (earliest deadline)
        task = self.task_queue[0]
        
        # Check if task is overdue
        if task.is_overdue():
            heapq.heappop(self.task_queue)
            task.status = TaskStatus.FAILED
            self.stats['deadline_misses'] += 1
            return None
        
        # Find capable robots
        candidates = self._evaluate_robot_candidates(task)
        
        if not candidates:
            return None
        
        # Select best robot
        best_robot = max(candidates, key=lambda r: r.total_score)
        
        # Assign task
        heapq.heappop(self.task_queue)
        task.status = TaskStatus.ASSIGNED
        task.assigned_robot = best_robot.robot_id
        
        self.stats['tasks_assigned'] += 1
        
        return (best_robot.robot_id, task)
    
    def _evaluate_robot_candidates(self, task: Task) -> List[RobotCandidate]:
        """Evaluate all robots for task assignment"""
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
                time_until_deadline=task.time_until_deadline()
            )
            
            candidates.append(RobotCandidate(
                robot_id=robot_id,
                battery_level=battery_level,
                position=robot_pos,
                battery_margin=battery_margin,
                total_score=total_score
            ))
        
        return candidates
    
    def _calculate_robot_score(
        self,
        battery_level: float,
        battery_margin: float,
        distance: float,
        time_until_deadline: float
    ) -> float:
        """
        Calculate composite robot selection score
        Higher score = better candidate
        """
        # Battery score (0-1)
        battery_score = battery_level / 100.0
        
        # Distance score (0-1, closer = better)
        distance_score = max(0.0, 1.0 - (distance / 50.0))
        
        # Deadline urgency score (0-1)
        deadline_score = min(1.0, time_until_deadline / 300.0)
        
        # Weighted combination
        total_score = (
            self.WEIGHT_BATTERY * battery_score +
            self.WEIGHT_DISTANCE * distance_score +
            self.WEIGHT_DEADLINE * deadline_score
        )
        
        return total_score
    
    def _estimate_travel_time(
        self,
        from_pos: Tuple[float, float],
        to_pos: Tuple[float, float]
    ) -> float:
        """Estimate travel time between positions"""
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
        battery_health: float
    ) -> float:
        """Estimate battery percentage consumed for task"""
        # Base discharge rates
        DISCHARGE_IDLE = 0.5
        DISCHARGE_MOVING = 2.0
        DISCHARGE_CARRYING = 3.5
        CAPACITY_AH = 50.0
        
        if task_type in ['PICKUP', 'DELIVER']:
            moving_time = duration * 0.7
            carrying_time = duration * 0.5
            idle_time = duration * 0.3
            
            total_discharge = (
                DISCHARGE_MOVING * moving_time +
                DISCHARGE_CARRYING * carrying_time +
                DISCHARGE_IDLE * idle_time
            ) / 3600.0
        else:
            total_discharge = (DISCHARGE_MOVING * duration) / 3600.0
        
        # Adjust for health
        health_factor = 1.0 + (100.0 - battery_health) / 100.0
        total_discharge *= health_factor
        
        return (total_discharge / CAPACITY_AH) * 100.0
    
    def should_charge(self, robot_id: str) -> bool:
        """Determine if robot should charge"""
        if robot_id not in self.robot_states:
            return False
        
        battery_level = self.robot_states[robot_id].get('battery_level', 100.0)
        
        # Force charging if critical
        if battery_level < self.BATTERY_CRITICAL:
            return True
        
        return False
    
    def assign_charging_station(
        self,
        robot_id: str,
        robot_position: Tuple[float, float]
    ) -> Optional[str]:
        """Assign robot to nearest available charging station"""
        best_station = None
        min_distance = float('inf')
        
        for station in self.charging_stations.values():
            if not station.occupied:
                distance = self._calculate_distance(robot_position, station.position)
                if distance < min_distance:
                    min_distance = distance
                    best_station = station
        
        if best_station:
            best_station.occupied = True
            best_station.occupied_by = robot_id
            return best_station.station_id
        
        return None
    
    def release_charging_station(self, robot_id: str, station_id: str):
        """Release charging station"""
        if station_id in self.charging_stations:
            station = self.charging_stations[station_id]
            if station.occupied_by == robot_id:
                station.occupied = False
                station.occupied_by = None
    
    def handle_robot_failure(self, robot_id: str):
        """Handle robot failure - reassign its task"""
        for task in self.task_queue:
            if task.assigned_robot == robot_id:
                task.retry_count += 1
                
                if task.retry_count >= self.MAX_TASK_RETRIES:
                    task.status = TaskStatus.FAILED
                    self.stats['tasks_failed'] += 1
                else:
                    task.status = TaskStatus.PENDING
                    task.assigned_robot = None
                    heapq.heapify(self.task_queue)
    
    def complete_task(self, task_id: str):
        """Mark task as completed"""
        for task in self.task_queue:
            if task.task_id == task_id:
                task.status = TaskStatus.COMPLETED
                
                if time.time() > task.deadline:
                    self.stats['deadline_misses'] += 1
                
                self.stats['tasks_completed'] += 1
                break
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        return self.stats.copy()