"""
Configuration Loader for Warehouse Cloud Robotics System
Supports dynamic robot count (1-10) and all system parameters
"""
import os
import yaml
from pathlib import Path
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
    edge_ttl_seconds: int = 3600
    cloud_bucket: str = 'robot-checkpoints'
    
    # Adaptive intervals
    high_battery_interval: float = 30.0
    medium_battery_interval: float = 15.0
    low_battery_interval: float = 5.0
 
@dataclass
class SchedulingConfig:
    """Task scheduling configuration"""
    algorithm: str = 'battery_aware_edf'
    task_timeout: float = 300.0
    battery_reserve: float = 15.0
    reassignment_delay: float = 5.0
    max_retries: int = 3
 
@dataclass
class NetworkConfig:
    """Network simulation configuration"""
    enable_latency_sim: bool = True
    base_latency_ms: float = 10.0
    latency_variance_ms: float = 5.0
    packet_loss_rate: float = 0.01
    bandwidth_mbps: float = 100.0
 
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
    obstacle_positions: list = field(default_factory=list)
 
@dataclass
class SystemConfig:
    """Overall system configuration"""
    num_robots: int = 6
    simulation_speed: float = 1.0
    real_time_factor: float = 1.0
    enable_visualization: bool = True
    enable_logging: bool = True
    log_level: str = 'INFO'
    experiment_mode: bool = False
 
@dataclass
class DatabaseConfig:
    """Database configuration"""
    postgres_host: str = 'localhost'
    postgres_port: int = 5432
    postgres_db: str = 'warehouse_robotics'
    postgres_user: str = 'robot_admin'
    postgres_password: str = 'robot_pass'
    
    redis_host: str = 'localhost'
    redis_port: int = 6379
    redis_db: int = 0
    
    minio_endpoint: str = 'localhost:9000'
    minio_access_key: str = 'minioadmin'
    minio_secret_key: str = 'minioadmin'
 
@dataclass
class Config:
    """Main configuration container"""
    system: SystemConfig = field(default_factory=SystemConfig)
    battery: BatteryConfig = field(default_factory=BatteryConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'Config':
        """Load configuration from YAML file"""
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls(
            system=SystemConfig(**data.get('system', {})),
            battery=BatteryConfig(**data.get('battery', {})),
            checkpoint=CheckpointConfig(**data.get('checkpoint', {})),
            scheduling=SchedulingConfig(**data.get('scheduling', {})),
            network=NetworkConfig(**data.get('network', {})),
            warehouse=WarehouseConfig(**data.get('warehouse', {})),
            database=DatabaseConfig(**data.get('database', {}))
        )
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        config = cls()
        
        # Override from environment
        if 'NUM_ROBOTS' in os.environ:
            config.system.num_robots = int(os.environ['NUM_ROBOTS'])
        
        if 'POSTGRES_HOST' in os.environ:
            config.database.postgres_host = os.environ['POSTGRES_HOST']
        
        if 'POSTGRES_PASSWORD' in os.environ:
            config.database.postgres_password = os.environ['POSTGRES_PASSWORD']
        
        if 'REDIS_HOST' in os.environ:
            config.database.redis_host = os.environ['REDIS_HOST']
        
        if 'MINIO_ENDPOINT' in os.environ:
            config.database.minio_endpoint = os.environ['MINIO_ENDPOINT']
        
        return config
    
    def validate(self):
        """Validate configuration values"""
        assert 1 <= self.system.num_robots <= 10, "num_robots must be between 1 and 10"
        assert self.battery.critical_threshold < self.battery.low_threshold
        assert self.checkpoint.default_tier in ['local', 'edge', 'cloud']
        assert self.scheduling.algorithm in ['edf', 'battery_aware_edf', 'fifo']
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'system': self.system.__dict__,
            'battery': self.battery.__dict__,
            'checkpoint': self.checkpoint.__dict__,
            'scheduling': self.scheduling.__dict__,
            'network': self.network.__dict__,
            'warehouse': self.warehouse.__dict__,
            'database': self.database.__dict__
        }
 
class ConfigLoader:
    """Singleton configuration loader"""
    _instance: Optional['ConfigLoader'] = None
    _config: Optional[Config] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def load(self, yaml_path: Optional[str] = None, use_env: bool = True) -> Config:
        """
        Load configuration with priority:
        1. Environment variables (highest)
        2. YAML file
        3. Defaults (lowest)
        """
        if self._config is not None:
            return self._config
        
        # Start with defaults
        if yaml_path and os.path.exists(yaml_path):
            self._config = Config.from_yaml(yaml_path)
        else:
            self._config = Config()
        
        # Override with environment variables
        if use_env:
            env_config = Config.from_env()
            # Merge configs (env takes priority)
            self._config.system.num_robots = env_config.system.num_robots
            self._config.database = env_config.database
        
        # Validate
        self._config.validate()
        
        return self._config
    
    def get_config(self) -> Config:
        """Get current configuration"""
        if self._config is None:
            return self.load()
        return self._config
    
    def reload(self, yaml_path: Optional[str] = None):
        """Reload configuration"""
        self._config = None
        return self.load(yaml_path)
 
# Global configuration instance
def get_config() -> Config:
    """Get global configuration instance"""
    loader = ConfigLoader()
    return loader.get_config()
 
# Example usage
if __name__ == '__main__':
    # Load from YAML
    config = get_config()
    
    print(f"System Configuration:")
    print(f"  Robots: {config.system.num_robots}")
    print(f"  Warehouse: {config.warehouse.grid_width}x{config.warehouse.grid_height}")
    print(f"  Charging Stations: {config.warehouse.charging_stations}")
    print(f"  Battery Critical: {config.battery.critical_threshold}%")
    print(f"  Checkpoint Tier: {config.checkpoint.default_tier}")
 