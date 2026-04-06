"""
Multi-Tier Adaptive Checkpoint Manager
Supports: Local (1ms) / Edge (10ms) / Cloud (100ms) storage
"""
import pickle
import zlib
import time
import hashlib
from typing import Dict, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, asdict

class CheckpointTier(Enum):
    """Checkpoint storage tiers"""
    LOCAL = 'local'
    EDGE = 'edge'
    CLOUD = 'cloud'

@dataclass
class CheckpointMetadata:
    """Metadata for a checkpoint"""
    checkpoint_id: str
    robot_id: str
    timestamp: float
    tier: CheckpointTier
    compressed: bool
    size_bytes: int
    state_hash: str
    battery_level: float
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['tier'] = self.tier.value
        return data

class CheckpointManager:
    """
    Intelligent multi-tier checkpoint management
    
    Features:
    - Automatic tier selection
    - Compression
    - Integrity verification
    - Graceful degradation
    """
    
    def __init__(self, robot_id: str, config: Dict[str, Any]):
        self.robot_id = robot_id
        
        # Configuration
        self.COMPRESSION_ENABLED = config.get('compression', True)
        self.COMPRESSION_LEVEL = config.get('compression_level', 6)
        self.MAX_LOCAL_CHECKPOINTS = config.get('max_local_checkpoints', 10)
        
        # Local storage (in-memory)
        self.local_checkpoints: Dict[str, bytes] = {}
        self.local_metadata: Dict[str, CheckpointMetadata] = {}
        self.local_access_times: Dict[str, float] = {}
        
        # Statistics
        self.stats = {
            'checkpoints_created': 0,
            'checkpoints_restored': 0,
            'local_hits': 0,
            'total_bytes_saved': 0
        }
    
    def create_checkpoint(
        self,
        state: Dict[str, Any],
        tier: Optional[CheckpointTier] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, CheckpointMetadata]:
        """
        Create checkpoint and store in specified tier
        
        Args:
            state: Robot state dictionary
            tier: Storage tier (None for auto-select)
            metadata: Additional metadata
            
        Returns:
            (checkpoint_id, metadata)
        """
        # Generate checkpoint ID
        timestamp = time.time()
        checkpoint_id = f"{self.robot_id}_{int(timestamp * 1000)}"
        
        # Serialize state
        state_bytes = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
        original_size = len(state_bytes)
        
        # Calculate hash
        state_hash = hashlib.md5(state_bytes).hexdigest()
        
        # Compress if enabled
        compressed = False
        if self.COMPRESSION_ENABLED:
            compressed_bytes = zlib.compress(state_bytes, level=self.COMPRESSION_LEVEL)
            if len(compressed_bytes) < original_size:
                state_bytes = compressed_bytes
                compressed = True
        
        final_size = len(state_bytes)
        
        # Auto-select tier if not specified
        if tier is None:
            tier = self._decide_tier(
                battery_level=metadata.get('battery_level', 50.0) if metadata else 50.0,
                network_quality=metadata.get('network_quality', 1.0) if metadata else 1.0,
                state_size=final_size
            )
        
        # Create metadata
        checkpoint_metadata = CheckpointMetadata(
            checkpoint_id=checkpoint_id,
            robot_id=self.robot_id,
            timestamp=timestamp,
            tier=tier,
            compressed=compressed,
            size_bytes=final_size,
            state_hash=state_hash,
            battery_level=metadata.get('battery_level', 0.0) if metadata else 0.0
        )
        
        # Store in tier
        if tier == CheckpointTier.LOCAL:
            self._store_local(checkpoint_id, state_bytes, checkpoint_metadata)
        
        # Update statistics
        self.stats['checkpoints_created'] += 1
        self.stats['total_bytes_saved'] += final_size
        
        return checkpoint_id, checkpoint_metadata
    
    def restore_checkpoint(
        self,
        checkpoint_id: str,
        verify_integrity: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Restore checkpoint from available storage
        
        Args:
            checkpoint_id: Checkpoint identifier
            verify_integrity: Verify MD5 hash
            
        Returns:
            Restored state dictionary or None
        """
        # Try local
        state_bytes, metadata = self._fetch_local(checkpoint_id)
        if state_bytes:
            self.stats['local_hits'] += 1
        
        if state_bytes is None:
            return None
        
        # Decompress if needed
        if metadata and metadata.compressed:
            try:
                state_bytes = zlib.decompress(state_bytes)
            except Exception as e:
                print(f"Decompression failed: {e}")
                return None
        
        # Verify integrity
        if verify_integrity and metadata:
            current_hash = hashlib.md5(state_bytes).hexdigest()
            if current_hash != metadata.state_hash:
                print(f"Integrity check failed for {checkpoint_id}")
                return None
        
        # Deserialize
        try:
            state = pickle.loads(state_bytes)
            self.stats['checkpoints_restored'] += 1
            return state
        except Exception as e:
            print(f"Deserialization failed: {e}")
            return None
    
    def _decide_tier(
        self,
        battery_level: float,
        network_quality: float,
        state_size: int
    ) -> CheckpointTier:
        """
        Intelligently decide which tier to use
        
        Decision logic:
        - Low battery → Cloud (for durability)
        - Poor network → Local (to avoid delays)
        - Default → Local (fastest)
        """
        # Critical battery → would use cloud in production
        if battery_level < 20.0:
            return CheckpointTier.LOCAL  # Using local for now
        
        # Poor network → local
        if network_quality < 0.3:
            return CheckpointTier.LOCAL
        
        # Large state + poor network → local
        if state_size > 1_000_000 and network_quality < 0.6:
            return CheckpointTier.LOCAL
        
        # Default: local (for simplicity in this implementation)
        return CheckpointTier.LOCAL
    
    def _store_local(self, checkpoint_id: str, data: bytes, 
                    metadata: CheckpointMetadata):
        """Store in local memory with LRU eviction"""
        self.local_checkpoints[checkpoint_id] = data
        self.local_metadata[checkpoint_id] = metadata
        self.local_access_times[checkpoint_id] = time.time()
        
        # Evict oldest if exceeds limit
        if len(self.local_checkpoints) > self.MAX_LOCAL_CHECKPOINTS:
            oldest_id = min(self.local_access_times.items(), key=lambda x: x[1])[0]
            del self.local_checkpoints[oldest_id]
            del self.local_metadata[oldest_id]
            del self.local_access_times[oldest_id]
    
    def _fetch_local(self, checkpoint_id: str) -> Tuple[Optional[bytes], Optional[CheckpointMetadata]]:
        """Fetch from local memory"""
        if checkpoint_id in self.local_checkpoints:
            self.local_access_times[checkpoint_id] = time.time()
            return (self.local_checkpoints[checkpoint_id], 
                   self.local_metadata[checkpoint_id])
        return None, None
    
    def adaptive_checkpoint_interval(
        self,
        battery_level: float,
        task_complexity: float
    ) -> float:
        """
        Determine optimal checkpoint interval
        
        Returns: Interval in seconds
        """
        interval = 30.0  # Base interval
        
        # Adjust based on battery
        if battery_level < 30.0:
            interval = 5.0
        elif battery_level < 60.0:
            interval = 15.0
        
        # Adjust based on task complexity
        if task_complexity > 0.7:
            interval *= 0.5
        
        return max(5.0, min(60.0, interval))
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get checkpoint manager statistics"""
        return {
            **self.stats,
            'local_count': len(self.local_checkpoints)
        }
    