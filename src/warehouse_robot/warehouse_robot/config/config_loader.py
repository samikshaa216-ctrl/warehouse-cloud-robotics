"""
Configuration Loader for Warehouse Cloud Robotics System
Supports dynamic robot count (1-10)
"""
import os
import yaml
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class BatteryConfig:
    """Battery system configuration"""
    initial_level: float = 100.0
    nominal_voltage: float = 48.0
    capacity_ah: float = 50.0
    critical_threshold: float = 20.0
    low_threshold: float = 40.0
    discharge_rate_idle: float = 0.5
    discharge_rate_moving: float = 2.0
    discharge_rate_carrying: float = 3.5
    charge_rate: float = 10.0

@dataclass
class CheckpointConfig:
    """Checkpointing system configuration"""
    enabled: bool = True
    default_tier: str = 'edge'
    compression: bool = True
    compression_level: int = 6
    max_local_checkpoints: int = 10

@dataclass
class SchedulingConfig:
    """Task scheduling configuration"""
    algorithm: str = 'battery_aware_edf'
    task_timeout: float = 300.0
    battery_reserve: float = 15.0
    max_retries: int = 3

@dataclass
class WarehouseConfig:
    """Warehouse environment configuration"""
    grid_width: int = 50
    grid_height: int = 30
    cell_size: float = 1.0
    charging_stations: int = 3
    charging_station_positions: list = field(default_factory=lambda: [
        (5, 5), (45, 5), (25, 25)
    ])

@dataclass
class SystemConfig:
    """Overall system configuration"""
    num_robots: int = 6
    simulation_speed: float = 1.0
    enable_visualization: bool = True
    enable_logging: bool = True
    log_level: str = 'INFO'

@dataclass
class Config:
    """Main configuration container"""
    system: SystemConfig = field(default_factory=SystemConfig)
    battery: BatteryConfig = field(default_factory=BatteryConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'Config':
        """Load configuration from YAML file"""
        if not os.path.exists(yaml_path):
            print(f"Warning: Config file {yaml_path} not found, using defaults")
            return cls()
            
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls(
            system=SystemConfig(**data.get('system', {})),
            battery=BatteryConfig(**data.get('battery', {})),
            checkpoint=CheckpointConfig(**data.get('checkpoint', {})),
            scheduling=SchedulingConfig(**data.get('scheduling', {})),
            warehouse=WarehouseConfig(**data.get('warehouse', {}))
        )
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        config = cls()
        if 'NUM_ROBOTS' in os.environ:
            config.system.num_robots = int(os.environ['NUM_ROBOTS'])
        return config

def get_config(yaml_path: Optional[str] = None) -> Config:
    """Get configuration instance"""
    if yaml_path and os.path.exists(yaml_path):
        return Config.from_yaml(yaml_path)
    return Config.from_env()