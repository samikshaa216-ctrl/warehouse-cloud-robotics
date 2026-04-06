"""
Multi-Tier Adaptive Checkpoint Manager
Supports three storage tiers:
- Local: In-memory (1ms latency, volatile)
- Edge: Redis (10ms latency, cluster-durable)
- Cloud: S3/MinIO (100ms latency, permanent)
"""
import pickle
import zlib
import time
import json
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from enum import Enum
from dataclasses import dataclass, asdict
import asyncio

# Optional imports (graceful degradation if not available)
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

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
    state_hash: str  # MD5 hash for integrity
    battery_level: float
    task_id: Optional[str]
    network_quality: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            'tier': self.tier.value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointMetadata':
        data['tier'] = CheckpointTier(data['tier'])
        return cls(**data)

class CheckpointManager:
    """
    Intelligent multi-tier checkpoint management system
    
    Features:
    - Automatic tier selection based on context
    - Compression for network efficiency
    - Delta checkpointing (future)
    - Integrity verification
    - Graceful degradation when tiers unavailable
    """
    
    def __init__(self, robot_id: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize checkpoint manager
        
        Args:
            robot_id: Unique robot identifier
            config: Configuration dictionary
        """
        self.robot_id = robot_id
        config = config or {}
        
        # Configuration
        self.COMPRESSION_ENABLED = config.get('compression', True)
        self.COMPRESSION_LEVEL = config.get('compression_level', 6)
        self.MAX_LOCAL_CHECKPOINTS = config.get('max_local_checkpoints', 10)
        self.EDGE_TTL_SECONDS = config.get('edge_ttl_seconds', 3600)
        self.CLOUD_BUCKET = config.get('cloud_bucket', 'robot-checkpoints')
        
        # Local storage (in-memory)
        self.local_checkpoints: Dict[str, bytes] = {}
        self.local_metadata: Dict[str, CheckpointMetadata] = {}
        self.local_access_times: Dict[str, float] = {}
        
        # Edge storage (Redis)
        self.redis_client: Optional[redis.Redis] = None
        self.redis_available = False
        
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(
                    host=config.get('redis_host', 'localhost'),
                    port=config.get('redis_port', 6379),
                    db=config.get('redis_db', 0),
                    decode_responses=False,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0
                )
                # Test connection
                self.redis_client.ping()
                self.redis_available = True
                print(f"[Checkpoint] Redis connected for robot {robot_id}")
            except Exception as e:
                print(f"[Checkpoint] Redis unavailable for robot {robot_id}: {e}")
                self.redis_available = False
        
        # Cloud storage (S3/MinIO)
        self.s3_client: Optional[Any] = None
        self.s3_available = False
        
        if S3_AVAILABLE:
            try:
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=config.get('minio_endpoint', 'http://localhost:9000'),
                    aws_access_key_id=config.get('minio_access_key', 'minioadmin'),
                    aws_secret_access_key=config.get('minio_secret_key', 'minioadmin'),
                    config=boto3.session.Config(signature_version='s3v4')
                )
                # Ensure bucket exists
                try:
                    self.s3_client.head_bucket(Bucket=self.CLOUD_BUCKET)
                except ClientError:
                    self.s3_client.create_bucket(Bucket=self.CLOUD_BUCKET)
                
                self.s3_available = True
                print(f"[Checkpoint] S3/MinIO connected for robot {robot_id}")
            except Exception as e:
                print(f"[Checkpoint] S3/MinIO unavailable for robot {robot_id}: {e}")
                self.s3_available = False
        
        # Statistics
        self.stats = {
            'checkpoints_created': 0,
            'checkpoints_restored': 0,
            'local_hits': 0,
            'edge_hits': 0,
            'cloud_hits': 0,
            'total_bytes_saved': 0,
            'compression_ratio': 0.0
        }
    
    def create_checkpoint(
        self,
        state: Dict[str, Any],
        tier: Optional[CheckpointTier] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, CheckpointMetadata]:
        """
        Create a checkpoint and store in specified or auto-selected tier
        
        Args:
            state: Robot state dictionary to checkpoint
            tier: Specific tier to use (None for auto-select)
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
        
        # Calculate state hash for integrity
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
                task_priority=metadata.get('task_priority', 3) if metadata else 3,
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
            battery_level=metadata.get('battery_level', 0.0) if metadata else 0.0,
            task_id=metadata.get('task_id') if metadata else None,
            network_quality=metadata.get('network_quality', 1.0) if metadata else 1.0
        )
        
        # Store in appropriate tier
        if tier == CheckpointTier.LOCAL:
            self._store_local(checkpoint_id, state_bytes, checkpoint_metadata)
        elif tier == CheckpointTier.EDGE:
            self._store_edge(checkpoint_id, state_bytes, checkpoint_metadata)
        elif tier == CheckpointTier.CLOUD:
            self._store_cloud(checkpoint_id, state_bytes, checkpoint_metadata)
        
        # Update statistics
        self.stats['checkpoints_created'] += 1
        self.stats['total_bytes_saved'] += final_size
        if compressed:
            self.stats['compression_ratio'] = 1.0 - (final_size / original_size)
        
        return checkpoint_id, checkpoint_metadata
    
    def restore_checkpoint(
        self,
        checkpoint_id: str,
        verify_integrity: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Restore checkpoint from any available tier
        
        Searches in order: local → edge → cloud
        
        Args:
            checkpoint_id: Checkpoint identifier
            verify_integrity: Verify MD5 hash after restoration
            
        Returns:
            Restored state dictionary or None if not found
        """
        # Try local first (fastest)
        state_bytes, metadata = self._fetch_local(checkpoint_id)
        if state_bytes:
            self.stats['local_hits'] += 1
        
        # Try edge
        if state_bytes is None:
            state_bytes, metadata = self._fetch_edge(checkpoint_id)
            if state_bytes:
                self.stats['edge_hits'] += 1
                # Cache in local for future access
                self._store_local(checkpoint_id, state_bytes, metadata)
        
        # Try cloud
        if state_bytes is None:
            state_bytes, metadata = self._fetch_cloud(checkpoint_id)
            if state_bytes:
                self.stats['cloud_hits'] += 1
                # Cache in edge and local
                self._store_edge(checkpoint_id, state_bytes, metadata)
                self._store_local(checkpoint_id, state_bytes, metadata)
        
        if state_bytes is None:
            return None
        
        # Decompress if needed
        if metadata and metadata.compressed:
            try:
                state_bytes = zlib.decompress(state_bytes)
            except Exception as e:
                print(f"[Checkpoint] Decompression failed for {checkpoint_id}: {e}")
                return None
        
        # Verify integrity
        if verify_integrity and metadata:
            current_hash = hashlib.md5(state_bytes).hexdigest()
            if current_hash != metadata.state_hash:
                print(f"[Checkpoint] Integrity check failed for {checkpoint_id}")
                return None
        
        # Deserialize
        try:
            state = pickle.loads(state_bytes)
            self.stats['checkpoints_restored'] += 1
            return state
        except Exception as e:
            print(f"[Checkpoint] Deserialization failed for {checkpoint_id}: {e}")
            return None
    
    def _decide_tier(
        self,
        battery_level: float,
        network_quality: float,
        task_priority: int,
        state_size: int
    ) -> CheckpointTier:
        """
        Intelligently decide which tier to use
        
        Decision factors:
        - Battery level (low = cloud for durability)
        - Network quality (poor = local to avoid delays)
        - Task priority (high = cloud for permanence)
        - State size (large + poor network = local)
        
        Args:
            battery_level: Current battery % (0-100)
            network_quality: Network quality score (0-1)
            task_priority: Task priority (1-5)
            state_size: Size of checkpoint in bytes
            
        Returns:
            Recommended checkpoint tier
        """
        # Critical battery → cloud for maximum durability
        if battery_level < 20.0:
            return CheckpointTier.CLOUD
        
        # Critical task → cloud for permanence
        if task_priority >= 4:
            return CheckpointTier.CLOUD
        
        # Poor network → local to avoid delays
        if network_quality < 0.3:
            return CheckpointTier.LOCAL
        
        # Large state + mediocre network → local
        if state_size > 1_000_000 and network_quality < 0.6:
            return CheckpointTier.LOCAL
        
        # Good network + cloud available → cloud for durability
        if network_quality > 0.8 and self.s3_available:
            return CheckpointTier.CLOUD
        
        # Default: edge (balanced latency and durability)
        return CheckpointTier.EDGE
    
    # --- Storage tier implementations ---
    
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
    
    def _store_edge(self, checkpoint_id: str, data: bytes,
                   metadata: CheckpointMetadata):
        """Store in Redis edge cache"""
        if not self.redis_available or self.redis_client is None:
            # Fallback to local
            self._store_local(checkpoint_id, data, metadata)
            return
        
        try:
            # Store checkpoint data
            data_key = f"checkpoint:data:{checkpoint_id}"
            self.redis_client.setex(data_key, self.EDGE_TTL_SECONDS, data)
            
            # Store metadata
            metadata_key = f"checkpoint:meta:{checkpoint_id}"
            metadata_json = json.dumps(metadata.to_dict())
            self.redis_client.setex(metadata_key, self.EDGE_TTL_SECONDS, metadata_json)
            
        except Exception as e:
            print(f"[Checkpoint] Redis store failed for {checkpoint_id}: {e}")
            self._store_local(checkpoint_id, data, metadata)
    
    def _fetch_edge(self, checkpoint_id: str) -> Tuple[Optional[bytes], Optional[CheckpointMetadata]]:
        """Fetch from Redis edge cache"""
        if not self.redis_available or self.redis_client is None:
            return None, None
        
        try:
            # Fetch data
            data_key = f"checkpoint:data:{checkpoint_id}"
            data = self.redis_client.get(data_key)
            
            if data is None:
                return None, None
            
            # Fetch metadata
            metadata_key = f"checkpoint:meta:{checkpoint_id}"
            metadata_json = self.redis_client.get(metadata_key)
            
            if metadata_json:
                metadata_dict = json.loads(metadata_json)
                metadata = CheckpointMetadata.from_dict(metadata_dict)
            else:
                metadata = None
            
            return data, metadata
            
        except Exception as e:
            print(f"[Checkpoint] Redis fetch failed for {checkpoint_id}: {e}")
            return None, None
    
    def _store_cloud(self, checkpoint_id: str, data: bytes,
                    metadata: CheckpointMetadata):
        """Store in S3/MinIO cloud storage"""
        if not self.s3_available or self.s3_client is None:
            # Fallback to edge
            self._store_edge(checkpoint_id, data, metadata)
            return
        
        try:
            # S3 object key
            data_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.pkl"
            metadata_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.meta.json"
            
            # Store checkpoint data
            self.s3_client.put_object(
                Bucket=self.CLOUD_BUCKET,
                Key=data_key,
                Body=data,
                Metadata={
                    'robot_id': self.robot_id,
                    'timestamp': str(metadata.timestamp),
                    'battery_level': str(metadata.battery_level)
                }
            )
            
            # Store metadata
            metadata_json = json.dumps(metadata.to_dict())
            self.s3_client.put_object(
                Bucket=self.CLOUD_BUCKET,
                Key=metadata_key,
                Body=metadata_json.encode('utf-8'),
                ContentType='application/json'
            )
            
        except Exception as e:
            print(f"[Checkpoint] S3 store failed for {checkpoint_id}: {e}")
            self._store_edge(checkpoint_id, data, metadata)
    
    def _fetch_cloud(self, checkpoint_id: str) -> Tuple[Optional[bytes], Optional[CheckpointMetadata]]:
        """Fetch from S3/MinIO cloud storage"""
        if not self.s3_available or self.s3_client is None:
            return None, None
        
        try:
            # S3 object keys
            data_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.pkl"
            metadata_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.meta.json"
            
            # Fetch checkpoint data
            response = self.s3_client.get_object(
                Bucket=self.CLOUD_BUCKET,
                Key=data_key
            )
            data = response['Body'].read()
            
            # Fetch metadata
            try:
                meta_response = self.s3_client.get_object(
                    Bucket=self.CLOUD_BUCKET,
                    Key=metadata_key
                )
                metadata_json = meta_response['Body'].read().decode('utf-8')
                metadata_dict = json.loads(metadata_json)
                metadata = CheckpointMetadata.from_dict(metadata_dict)
            except:
                metadata = None
            
            return data, metadata
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None, None
            print(f"[Checkpoint] S3 fetch failed for {checkpoint_id}: {e}")
            return None, None
    
    def list_checkpoints(self, tier: Optional[CheckpointTier] = None) -> List[CheckpointMetadata]:
        """List all available checkpoints"""
        checkpoints = []
        
        if tier is None or tier == CheckpointTier.LOCAL:
            checkpoints.extend(self.local_metadata.values())
        
        # TODO: Implement listing for edge and cloud
        
        return checkpoints
    
    def delete_checkpoint(self, checkpoint_id: str, tier: Optional[CheckpointTier] = None):
        """Delete checkpoint from specified tier or all tiers"""
        if tier is None or tier == CheckpointTier.LOCAL:
            self.local_checkpoints.pop(checkpoint_id, None)
            self.local_metadata.pop(checkpoint_id, None)
            self.local_access_times.pop(checkpoint_id, None)
        
        if tier is None or tier == CheckpointTier.EDGE:
            if self.redis_available and self.redis_client:
                try:
                    self.redis_client.delete(f"checkpoint:data:{checkpoint_id}")
                    self.redis_client.delete(f"checkpoint:meta:{checkpoint_id}")
                except:
                    pass
        
        if tier is None or tier == CheckpointTier.CLOUD:
            if self.s3_available and self.s3_client:
                try:
                    data_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.pkl"
                    metadata_key = f"checkpoints/{self.robot_id}/{checkpoint_id}.meta.json"
                    self.s3_client.delete_object(Bucket=self.CLOUD_BUCKET, Key=data_key)
                    self.s3_client.delete_object(Bucket=self.CLOUD_BUCKET, Key=metadata_key)
                except:
                    pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get checkpoint manager statistics"""
        return {
            **self.stats,
            'local_count': len(self.local_checkpoints),
            'redis_available': self.redis_available,
            's3_available': self.s3_available
        }
    
    def adaptive_checkpoint_interval(
        self,
        battery_level: float,
        network_quality: float,
        task_complexity: float
    ) -> float:
        """
        Determine optimal checkpoint interval based on context
        
        Returns: Interval in seconds
        """
        # Base interval
        interval = 30.0
        
        # Adjust based on battery
        if battery_level < 30.0:
            interval = 5.0  # Frequent when low
        elif battery_level < 60.0:
            interval = 15.0
        
        # Adjust based on network
        if network_quality < 0.3:
            interval *= 2.0  # Less frequent if network poor
        
        # Adjust based on task complexity
        if task_complexity > 0.7:
            interval *= 0.5  # More frequent for complex tasks
        
        return max(5.0, min(60.0, interval))  # Clamp to 5-60s

# Example usage
if __name__ == '__main__':
    # Test checkpoint manager
    manager = CheckpointManager('robot_test_001')
    
    # Create a test state
    test_state = {
        'position': [10.5, 23.8],
        'velocity': [0.5, 0.3],
        'task_id': 'task_123',
        'task_progress': 0.75,
        'path': [[10, 20], [11, 21], [12, 22]]
    }
    
    print("Creating checkpoint...")
    checkpoint_id, metadata = manager.create_checkpoint(
        state=test_state,
        metadata={
            'battery_level': 45.0,
            'network_quality': 0.8,
            'task_priority': 3
        }
    )
    
    print(f"Checkpoint created: {checkpoint_id}")
    print(f"Tier: {metadata.tier.value}")
    print(f"Compressed: {metadata.compressed}")
    print(f"Size: {metadata.size_bytes} bytes")
    
    print("\nRestoring checkpoint...")
    restored_state = manager.restore_checkpoint(checkpoint_id)
    
    if restored_state:
        print("Checkpoint restored successfully!")
        print(f"Position: {restored_state['position']}")
        print(f"Task progress: {restored_state['task_progress']}")
    
    print("\nStatistics:")
    stats = manager.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")