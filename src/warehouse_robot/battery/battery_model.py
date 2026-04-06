"""
Advanced Battery Model for Warehouse Robots
Features:
- Realistic lithium-ion discharge characteristics
- Temperature modeling
- Health degradation
- Predictive drain estimation
- Charging cycle tracking
"""
import math
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
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
    total_discharge_ah: float  # Total amp-hours discharged
    time_to_empty: float  # Estimated seconds until empty
    time_to_full: float  # Estimated seconds until fully charged
    is_charging: bool
    is_critical: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatteryState':
        """Create from dictionary"""
        return cls(**data)

class BatteryModel:
    """
    Sophisticated battery discharge model
    Based on lithium-ion battery physics with practical simplifications
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize battery model
        
        Args:
            config: Battery configuration dictionary
        """
        # Load config or use defaults
        config = config or {}
        
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
        self.TEMP_DEGRADATION_RATE = 0.001  # Health loss per second over threshold
        
        # Environmental parameters
        self.AMBIENT_TEMP = 25.0  # Celsius
        self.MAX_TEMP = 45.0  # Maximum safe temperature
        self.THERMAL_TIME_CONSTANT = 60.0  # Seconds
        
        # Initialize state
        self.state = BatteryState(
            level=self.INITIAL_LEVEL,
            voltage=self._calculate_voltage(self.INITIAL_LEVEL),
            current=0.0,
            temperature=self.AMBIENT_TEMP,
            health=100.0,
            charge_cycles=0,
            total_discharge_ah=0.0,
            time_to_empty=float('inf'),
            time_to_full=0.0,
            is_charging=False,
            is_critical=False
        )
        
        self.last_update_time = time.time()
        
    def update(self, dt: float, robot_state: RobotState, 
              carrying_load: bool = False) -> BatteryState:
        """
        Update battery state based on robot activity
        
        Args:
            dt: Time delta in seconds
            robot_state: Current robot state
            carrying_load: Whether robot is carrying a payload
            
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
            elif robot_state == RobotState.MOVING or robot_state == RobotState.CARRYING:
                if carrying_load or robot_state == RobotState.CARRYING:
                    discharge_rate = self.DISCHARGE_RATE_CARRYING
                else:
                    discharge_rate = self.DISCHARGE_RATE_MOVING
            else:
                discharge_rate = self.DISCHARGE_RATE_IDLE
            
            self._discharge(dt, discharge_rate)
        
        # Update thermal model
        self._update_temperature(dt, robot_state)
        
        # Update health degradation
        self._update_health(dt)
        
        # Update predictive metrics
        self._update_predictions()
        
        # Update status flags
        self.state.is_critical = self.state.level < self.CRITICAL_THRESHOLD
        
        return self.state
    
    def _discharge(self, dt: float, base_rate: float):
        """
        Discharge battery
        
        Args:
            dt: Time delta in seconds
            base_rate: Base discharge rate in amps
        """
        # Apply health degradation factor
        health_factor = 1.0 + (100.0 - self.state.health) / 100.0
        effective_rate = base_rate * health_factor
        
        # Calculate energy consumed
        energy_ah = effective_rate * (dt / 3600.0)
        
        # Update total discharge
        self.state.total_discharge_ah += energy_ah
        
        # Update level
        level_decrease = (energy_ah / self.CAPACITY_AH) * 100.0
        self.state.level = max(0.0, self.state.level - level_decrease)
        
        # Update voltage (non-linear discharge curve)
        self.state.voltage = self._calculate_voltage(self.state.level)
        
        # Update current
        self.state.current = -effective_rate  # Negative for discharge
        
        self.state.is_charging = False
    
    def _charge(self, dt: float):
        """
        Charge battery
        
        Args:
            dt: Time delta in seconds
        """
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
        self.state.current = charge_rate  # Positive for charge
        
        self.state.is_charging = True
    
    def _update_temperature(self, dt: float, robot_state: RobotState):
        """
        Update battery temperature using thermal model
        
        Args:
            dt: Time delta in seconds
            robot_state: Current robot state
        """
        # Heat generation based on state
        if robot_state == RobotState.CHARGING:
            heat_generation = 0.1  # Celsius/second
        elif robot_state == RobotState.MOVING or robot_state == RobotState.CARRYING:
            heat_generation = 0.05
        else:
            heat_generation = 0.0
        
        # Cooling toward ambient (exponential decay)
        temp_diff = self.state.temperature - self.AMBIENT_TEMP
        cooling_rate = -temp_diff / self.THERMAL_TIME_CONSTANT
        
        # Total temperature change
        temp_change = (heat_generation + cooling_rate) * dt
        self.state.temperature += temp_change
        
        # Clamp to physical limits
        self.state.temperature = max(self.AMBIENT_TEMP, 
                                    min(self.MAX_TEMP, self.state.temperature))
    
    def _update_health(self, dt: float):
        """
        Update battery health based on temperature and usage
        
        Args:
            dt: Time delta in seconds
        """
        # Temperature-based degradation
        if self.state.temperature > self.TEMP_DEGRADATION_THRESHOLD:
            temp_excess = self.state.temperature - self.TEMP_DEGRADATION_THRESHOLD
            degradation = self.TEMP_DEGRADATION_RATE * temp_excess * dt
            self.state.health -= degradation
            self.state.health = max(0.0, self.state.health)
    
    def _calculate_voltage(self, level: float) -> float:
        """
        Calculate voltage from charge level (non-linear discharge curve)
        
        Args:
            level: Battery level (0-100)
            
        Returns:
            Voltage
        """
        # Simplified Li-ion discharge curve
        # Voltage drops more rapidly at low charge
        normalized_level = level / 100.0
        
        if normalized_level > 0.2:
            # Linear region (80% of voltage range)
            voltage_fraction = 0.8 + 0.2 * normalized_level
        else:
            # Rapid drop below 20%
            voltage_fraction = 0.8 * (normalized_level / 0.2)
        
        return self.NOMINAL_VOLTAGE * voltage_fraction
    
    def _update_predictions(self):
        """Update predictive time estimates"""
        if self.state.is_charging:
            # Time to full
            if self.state.current > 0:
                remaining_ah = self.CAPACITY_AH * (100.0 - self.state.level) / 100.0
                self.state.time_to_full = (remaining_ah / self.state.current) * 3600.0
            else:
                self.state.time_to_full = 0.0
            self.state.time_to_empty = float('inf')
        else:
            # Time to empty
            if abs(self.state.current) > 0:
                remaining_ah = self.CAPACITY_AH * self.state.level / 100.0
                self.state.time_to_empty = (remaining_ah / abs(self.state.current)) * 3600.0
            else:
                self.state.time_to_empty = float('inf')
            self.state.time_to_full = 0.0
    
    def estimate_drain_for_task(self, task_duration: float, 
                                task_type: str) -> float:
        """
        Estimate battery drain for a given task
        
        Args:
            task_duration: Task duration in seconds
            task_type: Type of task (affects discharge rate)
            
        Returns:
            Estimated battery percentage consumed
        """
        # Estimate discharge rate based on task type
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
        Check if robot has enough battery to complete task safely
        
        Args:
            task_duration: Task duration in seconds
            task_type: Type of task
            safety_margin: Minimum battery % to maintain after task
            
        Returns:
            True if task can be completed safely
        """
        estimated_drain = self.estimate_drain_for_task(task_duration, task_type)
        return (self.state.level - estimated_drain) >= safety_margin
    
    def time_until_critical(self, current_state: RobotState = RobotState.MOVING) -> float:
        """
        Estimate time until battery reaches critical level
        
        Args:
            current_state: Assumed robot state for prediction
            
        Returns:
            Time in seconds until critical level
        """
        if self.state.level <= self.CRITICAL_THRESHOLD:
            return 0.0
        
        # Determine discharge rate
        if current_state == RobotState.IDLE:
            discharge_rate = self.DISCHARGE_RATE_IDLE
        elif current_state == RobotState.MOVING:
            discharge_rate = self.DISCHARGE_RATE_MOVING
        elif current_state == RobotState.CARRYING:
            discharge_rate = self.DISCHARGE_RATE_CARRYING
        else:
            discharge_rate = self.DISCHARGE_RATE_IDLE
        
        # Apply health factor
        health_factor = 1.0 + (100.0 - self.state.health) / 100.0
        effective_discharge = discharge_rate * health_factor
        
        # Calculate time
        level_to_drain = self.state.level - self.CRITICAL_THRESHOLD
        energy_to_drain_ah = (level_to_drain / 100.0) * self.CAPACITY_AH
        
        if effective_discharge > 0:
            time_hours = energy_to_drain_ah / effective_discharge
            return time_hours * 3600.0
        else:
            return float('inf')
    
    def get_state_snapshot(self) -> BatteryState:
        """Get current battery state snapshot"""
        return BatteryState(**asdict(self.state))
    
    def restore_state(self, state: BatteryState):
        """Restore battery state from snapshot"""
        self.state = state

# Example usage and testing
if __name__ == '__main__':
    battery = BatteryModel()
    
    print("Battery Model Test")
    print("=" * 50)
    
    # Simulate 1 hour of operation
    print("\nSimulating 1 hour of mixed operation...")
    
    for second in range(3600):
        if second < 1800:
            # First 30 minutes: moving
            battery.update(1.0, RobotState.MOVING)
        elif second < 2700:
            # Next 15 minutes: carrying
            battery.update(1.0, RobotState.CARRYING)
        else:
            # Last 15 minutes: idle
            battery.update(1.0, RobotState.IDLE)
        
        # Print status every 10 minutes
        if second % 600 == 0 and second > 0:
            print(f"\nAfter {second/60:.0f} minutes:")
            print(f"  Level: {battery.state.level:.1f}%")
            print(f"  Voltage: {battery.state.voltage:.2f}V")
            print(f"  Temperature: {battery.state.temperature:.1f}°C")
            print(f"  Health: {battery.state.health:.2f}%")
            print(f"  Time to critical: {battery.time_until_critical()/60:.1f} min")
    
    print("\n" + "=" * 50)
    print("Final State:")
    print(f"  Level: {battery.state.level:.1f}%")
    print(f"  Health: {battery.state.health:.2f}%")
    print(f"  Total discharged: {battery.state.total_discharge_ah:.2f} Ah")