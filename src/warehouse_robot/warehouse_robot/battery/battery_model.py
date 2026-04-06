"""
Advanced Battery Model for Warehouse Robots
Features: Realistic discharge, temperature, health degradation
"""
import time
import math
from dataclasses import dataclass, asdict
from typing import Dict, Any
from enum import Enum

class RobotState(Enum):
    """Robot operational states"""
    IDLE = 'idle'
    MOVING = 'moving'
    CARRYING = 'carrying'
    CHARGING = 'charging'
    CRASHED = 'crashed'

@dataclass
class BatteryState:
    """Complete battery state snapshot"""
    level: float  # 0.0 to 100.0
    voltage: float  # Current voltage
    current: float  # Current draw in amps
    temperature: float  # Celsius
    health: float  # Battery health percentage (0-100)
    charge_cycles: int  # Total charge cycles
    time_to_critical: float  # Estimated seconds until critical
    is_charging: bool
    is_critical: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

class BatteryModel:
    """
    Realistic battery discharge model
    Based on lithium-ion battery characteristics
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize battery model with configuration"""
        # Battery specifications
        self.NOMINAL_VOLTAGE = config.get('nominal_voltage', 48.0)
        self.CAPACITY_AH = config.get('capacity_ah', 50.0)
        self.INITIAL_LEVEL = config.get('initial_level', 100.0)
        
        # Discharge rates (Amps)
        self.DISCHARGE_RATE_IDLE = config.get('discharge_rate_idle', 0.5)
        self.DISCHARGE_RATE_MOVING = config.get('discharge_rate_moving', 2.0)
        self.DISCHARGE_RATE_CARRYING = config.get('discharge_rate_carrying', 3.5)
        
        # Charge rate
        self.CHARGE_RATE = config.get('charge_rate', 10.0)
        
        # Thresholds
        self.CRITICAL_THRESHOLD = config.get('critical_threshold', 20.0)
        self.LOW_THRESHOLD = config.get('low_threshold', 40.0)
        
        # Degradation parameters
        self.CYCLE_DEGRADATION = 0.0001  # 0.01% per cycle
        self.TEMP_DEGRADATION_THRESHOLD = 40.0  # Celsius
        
        # Environmental
        self.AMBIENT_TEMP = 25.0  # Celsius
        self.MAX_TEMP = 45.0
        
        # Initialize state
        self.state = BatteryState(
            level=self.INITIAL_LEVEL,
            voltage=self._calculate_voltage(self.INITIAL_LEVEL),
            current=0.0,
            temperature=self.AMBIENT_TEMP,
            health=100.0,
            charge_cycles=0,
            time_to_critical=float('inf'),
            is_charging=False,
            is_critical=False
        )
    
    def update(self, dt: float, robot_state: RobotState) -> BatteryState:
        """
        Update battery state based on robot activity
        
        Args:
            dt: Time delta in seconds
            robot_state: Current robot state
            
        Returns:
            Updated battery state
        """
        if robot_state == RobotState.CHARGING:
            self._charge(dt)
        elif robot_state == RobotState.CRASHED:
            # Minimal discharge when crashed
            self._discharge(dt, self.DISCHARGE_RATE_IDLE * 0.1)
        else:
            # Determine discharge rate
            if robot_state == RobotState.IDLE:
                discharge_rate = self.DISCHARGE_RATE_IDLE
            elif robot_state == RobotState.CARRYING:
                discharge_rate = self.DISCHARGE_RATE_CARRYING
            else:  # MOVING
                discharge_rate = self.DISCHARGE_RATE_MOVING
            
            self._discharge(dt, discharge_rate)
        
        # Update thermal model
        self._update_temperature(dt, robot_state)
        
        # Update health
        self._update_health(dt)
        
        # Update predictions
        self._update_predictions(robot_state)
        
        # Update flags
        self.state.is_critical = self.state.level < self.CRITICAL_THRESHOLD
        
        return self.state
    
    def _discharge(self, dt: float, base_rate: float):
        """Discharge battery"""
        # Apply health degradation factor
        health_factor = 1.0 + (100.0 - self.state.health) / 100.0
        effective_rate = base_rate * health_factor
        
        # Calculate energy consumed
        energy_ah = effective_rate * (dt / 3600.0)
        
        # Update level
        level_decrease = (energy_ah / self.CAPACITY_AH) * 100.0
        self.state.level = max(0.0, self.state.level - level_decrease)
        
        # Update voltage
        self.state.voltage = self._calculate_voltage(self.state.level)
        
        # Update current
        self.state.current = -effective_rate
        
        self.state.is_charging = False
    
    def _charge(self, dt: float):
        """Charge battery"""
        # Charging slows down near 100% (CC-CV charging)
        if self.state.level > 90.0:
            charge_rate = self.CHARGE_RATE * (100.0 - self.state.level) / 10.0
        else:
            charge_rate = self.CHARGE_RATE
        
        # Calculate energy added
        energy_ah = charge_rate * (dt / 3600.0)
        
        # Update level
        old_level = self.state.level
        level_increase = (energy_ah / self.CAPACITY_AH) * 100.0
        self.state.level = min(100.0, self.state.level + level_increase)
        
        # Increment charge cycle when crossing 100%
        if old_level < 100.0 and self.state.level == 100.0:
            self.state.charge_cycles += 1
            self.state.health -= self.CYCLE_DEGRADATION * 100.0
            self.state.health = max(0.0, self.state.health)
        
        # Update voltage
        self.state.voltage = self._calculate_voltage(self.state.level)
        
        # Update current
        self.state.current = charge_rate
        
        self.state.is_charging = True
    
    def _update_temperature(self, dt: float, robot_state: RobotState):
        """Update battery temperature"""
        # Heat generation based on state
        if robot_state == RobotState.CHARGING:
            heat_generation = 0.1  # Celsius/second
        elif robot_state in [RobotState.MOVING, RobotState.CARRYING]:
            heat_generation = 0.05
        else:
            heat_generation = 0.0
        
        # Cooling toward ambient
        temp_diff = self.state.temperature - self.AMBIENT_TEMP
        cooling_rate = -temp_diff / 60.0  # 60 second time constant
        
        # Total temperature change
        temp_change = (heat_generation + cooling_rate) * dt
        self.state.temperature += temp_change
        
        # Clamp to limits
        self.state.temperature = max(self.AMBIENT_TEMP, 
                                    min(self.MAX_TEMP, self.state.temperature))
    
    def _update_health(self, dt: float):
        """Update battery health"""
        # Temperature-based degradation
        if self.state.temperature > self.TEMP_DEGRADATION_THRESHOLD:
            temp_excess = self.state.temperature - self.TEMP_DEGRADATION_THRESHOLD
            degradation = 0.001 * temp_excess * dt
            self.state.health -= degradation
            self.state.health = max(0.0, self.state.health)
    
    def _calculate_voltage(self, level: float) -> float:
        """Calculate voltage from charge level (non-linear discharge curve)"""
        normalized_level = level / 100.0
        
        if normalized_level > 0.2:
            voltage_fraction = 0.8 + 0.2 * normalized_level
        else:
            voltage_fraction = 0.8 * (normalized_level / 0.2)
        
        return self.NOMINAL_VOLTAGE * voltage_fraction
    
    def _update_predictions(self, robot_state: RobotState):
        """Update predictive time estimates"""
        if self.state.is_charging:
            self.state.time_to_critical = float('inf')
        else:
            # Estimate time to critical level
            if self.state.level <= self.CRITICAL_THRESHOLD:
                self.state.time_to_critical = 0.0
            else:
                # Determine discharge rate
                if robot_state == RobotState.IDLE:
                    rate = self.DISCHARGE_RATE_IDLE
                elif robot_state == RobotState.MOVING:
                    rate = self.DISCHARGE_RATE_MOVING
                elif robot_state == RobotState.CARRYING:
                    rate = self.DISCHARGE_RATE_CARRYING
                else:
                    rate = self.DISCHARGE_RATE_IDLE
                
                # Calculate time
                level_to_drain = self.state.level - self.CRITICAL_THRESHOLD
                energy_ah = (level_to_drain / 100.0) * self.CAPACITY_AH
                
                if rate > 0:
                    self.state.time_to_critical = (energy_ah / rate) * 3600.0
                else:
                    self.state.time_to_critical = float('inf')
    
    def estimate_drain_for_task(self, task_duration: float, task_type: str) -> float:
        """
        Estimate battery drain for a given task
        
        Args:
            task_duration: Task duration in seconds
            task_type: Type of task
            
        Returns:
            Estimated battery percentage consumed
        """
        if task_type in ['PICKUP', 'DELIVER', 'TRANSPORT']:
            # Assume 70% moving, 50% carrying
            moving_time = task_duration * 0.7
            carrying_time = task_duration * 0.5
            idle_time = task_duration * 0.3
            
            total_discharge_ah = (
                (self.DISCHARGE_RATE_MOVING * moving_time +
                 self.DISCHARGE_RATE_CARRYING * carrying_time +
                 self.DISCHARGE_RATE_IDLE * idle_time) / 3600.0
            )
        else:
            # Generic movement
            total_discharge_ah = (self.DISCHARGE_RATE_MOVING * task_duration) / 3600.0
        
        # Apply health degradation
        health_factor = 1.0 + (100.0 - self.state.health) / 100.0
        effective_discharge = total_discharge_ah * health_factor
        
        return (effective_discharge / self.CAPACITY_AH) * 100.0
    
    def can_complete_task(self, task_duration: float, task_type: str,
                         safety_margin: float = 15.0) -> bool:
        """
        Check if robot has enough battery to complete task
        
        Args:
            task_duration: Task duration in seconds
            task_type: Type of task
            safety_margin: Minimum battery % to maintain after task
            
        Returns:
            True if task can be completed safely
        """
        estimated_drain = self.estimate_drain_for_task(task_duration, task_type)
        return (self.state.level - estimated_drain) >= safety_margin
    
    def get_state_snapshot(self) -> BatteryState:
        """Get current battery state snapshot"""
        return BatteryState(**asdict(self.state))