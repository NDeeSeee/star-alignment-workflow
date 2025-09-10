#!/usr/bin/env python3
"""
Super-Advanced HPC Resource Manager
Intelligent, predictive, and adaptive resource management for HPC environments
"""

import os
import json
import subprocess
import psutil
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging

# Import advanced features
try:
    from .advanced_features import StorageFailurePredictor, ResourceExhaustionPredictor, NetworkTopologyAwareness, HPCManagementIntegration
except ImportError:
    from advanced_features import StorageFailurePredictor, ResourceExhaustionPredictor, NetworkTopologyAwareness, HPCManagementIntegration

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class JobRequirements:
    """Job requirements specification"""
    cpus: int
    memory_gb: int
    walltime_hours: float
    storage_gb: float
    priority: int = 1
    queue_preference: Optional[str] = None

@dataclass
class ResourceAllocation:
    """Optimal resource allocation"""
    cpus: int
    memory_gb: int
    walltime_hours: float
    queue: str
    estimated_cost: float
    estimated_duration_hours: float
    success_probability: float

class AdvancedResourceManager:
    """Super-advanced HPC resource manager with predictive capabilities"""
    
    def __init__(self, workflow_dir: Path):
        self.workflow_dir = Path(workflow_dir)
        self.config_file = self.workflow_dir / "data" / "advanced_resource_config.json"
        self.history_file = self.workflow_dir / "data" / "resource_history.json"
        
        # Initialize components
        self.load_config()
        self.load_history()
        self.queue_manager = AdvancedQueueManager(self.config)
        self.predictor = ResourcePredictor(self.history)
        self.cost_optimizer = CostOptimizer(self.config)
        self.workload_balancer = WorkloadBalancer(self.config)
        
        # Initialize advanced features
        self.storage_predictor = StorageFailurePredictor(self.config)
        self.exhaustion_predictor = ResourceExhaustionPredictor(self.config)
        self.network_topology = NetworkTopologyAwareness(self.config)
        self.hpc_integration = HPCManagementIntegration(self.config)
        
        # Start real-time monitoring
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
        
    def load_config(self):
        """Load advanced configuration"""
        default_config = {
            "hpc_environment": {
                "cluster_name": "your_cluster",
                "scheduler": "LSF",
                "queues": {
                    "normal": {
                        "max_jobs": 100,
                        "max_cpu_per_job": 16,
                        "max_memory_per_job": 256,
                        "max_walltime_hours": 168,
                        "cost_per_cpu_hour": 0.1,
                        "priority": 1,
                        "speed_factor": 1.0
                    },
                    "hiprio": {
                        "max_jobs": 50,
                        "max_cpu_per_job": 32,
                        "max_memory_per_job": 512,
                        "max_walltime_hours": 72,
                        "cost_per_cpu_hour": 0.2,
                        "priority": 2,
                        "speed_factor": 1.5
                    },
                    "long": {
                        "max_jobs": 20,
                        "max_cpu_per_job": 8,
                        "max_memory_per_job": 128,
                        "max_walltime_hours": 720,
                        "cost_per_cpu_hour": 0.05,
                        "priority": 3,
                        "speed_factor": 0.7
                    },
                    "gpu": {
                        "max_jobs": 10,
                        "max_cpu_per_job": 16,
                        "max_memory_per_job": 256,
                        "max_walltime_hours": 48,
                        "cost_per_cpu_hour": 0.5,
                        "priority": 4,
                        "speed_factor": 2.0
                    }
                }
            },
            "optimization": {
                "chunk_strategies": {
                    "storage_optimized": {
                        "max_chunk_size": 500,
                        "storage_threshold": 0.85,
                        "priority": "storage_efficiency"
                    },
                    "time_optimized": {
                        "max_chunk_size": 2000,
                        "storage_threshold": 0.95,
                        "priority": "speed"
                    },
                    "cost_optimized": {
                        "max_chunk_size": 1000,
                        "storage_threshold": 0.90,
                        "priority": "cost_efficiency"
                    },
                    "balanced": {
                        "max_chunk_size": 1000,
                        "storage_threshold": 0.90,
                        "priority": "balanced"
                    }
                },
                "prediction": {
                    "enable_ml_prediction": True,
                    "prediction_horizon_hours": 24,
                    "confidence_threshold": 0.8,
                    "model_update_frequency": "daily"
                },
                "cost_optimization": {
                    "max_cost_per_job": 100.0,
                    "cost_weight": 0.3,
                    "time_weight": 0.4,
                    "reliability_weight": 0.3
                }
            },
            "monitoring": {
                "update_interval_seconds": 30,
                "history_retention_days": 30,
                "alert_thresholds": {
                    "storage_usage": 0.95,
                    "queue_wait_time": 24,
                    "job_failure_rate": 0.1
                }
            }
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                # Merge with defaults
                self._merge_config(default_config, self.config)
            except (json.JSONDecodeError, IOError):
                self.config = default_config
        else:
            self.config = default_config
            
        self.save_config()
        
    def _merge_config(self, default: dict, current: dict):
        """Recursively merge configuration with defaults"""
        for key, value in default.items():
            if key not in current:
                current[key] = value
            elif isinstance(value, dict) and isinstance(current[key], dict):
                self._merge_config(value, current[key])
                
    def load_history(self):
        """Load historical resource data"""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    self.history = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.history = {
                    "job_history": [],
                    "resource_usage": [],
                    "queue_performance": {},
                    "cost_history": []
                }
        else:
            self.history = {
                "job_history": [],
                "resource_usage": [],
                "queue_performance": {},
                "cost_history": []
            }
            
    def save_config(self):
        """Save configuration"""
        self.config_file.parent.mkdir(exist_ok=True)
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
            
    def save_history(self):
        """Save historical data"""
        with open(self.history_file, 'w') as f:
            json.dump(self.history, f, indent=2)
            
    def get_current_resources(self) -> Dict:
        """Get current system resource status"""
        try:
            # CPU information
            cpu_count = psutil.cpu_count()
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory information
            memory = psutil.virtual_memory()
            
            # Disk information
            disk = psutil.disk_usage(self.workflow_dir)
            
            # LSF queue information
            lsf_info = self._get_lsf_queue_info()
            
            # Network information
            network_info = self._get_network_info()
            
            resources = {
                "timestamp": datetime.now().isoformat(),
                "cpu": {
                    "count": cpu_count,
                    "usage_percent": cpu_percent,
                    "available": max(0, cpu_count - int(cpu_count * cpu_percent / 100))
                },
                "memory": {
                    "total_gb": memory.total / (1024**3),
                    "available_gb": memory.available / (1024**3),
                    "usage_percent": memory.percent
                },
                "storage": {
                    "total_gb": disk.total / (1024**3),
                    "used_gb": disk.used / (1024**3),
                    "free_gb": disk.free / (1024**3),
                    "usage_percent": (disk.used / disk.total) * 100
                },
                "lsf": lsf_info,
                "network": network_info
            }
            
            # Store in history
            self.history["resource_usage"].append(resources)
            if len(self.history["resource_usage"]) > 1000:  # Keep last 1000 entries
                self.history["resource_usage"] = self.history["resource_usage"][-1000:]
                
            return resources
            
        except Exception as e:
            logger.error(f"Error getting system resources: {e}")
            return None
            
    def _get_lsf_queue_info(self) -> Dict:
        """Get detailed LSF queue information"""
        try:
            # Get queue status
            result = subprocess.run(['bqueues', '-w'], capture_output=True, text=True)
            if result.returncode != 0:
                return {}
                
            lines = result.stdout.strip().split('\n')
            queue_info = {}
            
            for line in lines[1:]:  # Skip header
                parts = line.split()
                if len(parts) >= 6:
                    queue_name = parts[0]
                    queue_info[queue_name] = {
                        "pending": int(parts[1]) if parts[1].isdigit() else 0,
                        "running": int(parts[2]) if parts[2].isdigit() else 0,
                        "suspended": int(parts[3]) if parts[3].isdigit() else 0,
                        "max_jobs": int(parts[4]) if parts[4].isdigit() else 0,
                        "cpu_limit": int(parts[5]) if parts[5].isdigit() else 0
                    }
                    
            return queue_info
            
        except Exception as e:
            logger.error(f"Error getting LSF queue info: {e}")
            return {}
            
    def _get_network_info(self) -> Dict:
        """Get network information"""
        try:
            network_stats = psutil.net_io_counters()
            return {
                "bytes_sent": network_stats.bytes_sent,
                "bytes_recv": network_stats.bytes_recv,
                "packets_sent": network_stats.packets_sent,
                "packets_recv": network_stats.packets_recv
            }
        except Exception as e:
            logger.error(f"Error getting network info: {e}")
            return {}
            
    def calculate_optimal_chunk_size(self, total_samples: int, strategy: str = "balanced") -> int:
        """Calculate optimal chunk size using advanced strategies"""
        current_resources = self.get_current_resources()
        if not current_resources:
            return self.config["optimization"]["chunk_strategies"]["balanced"]["max_chunk_size"]
            
        strategy_config = self.config["optimization"]["chunk_strategies"][strategy]
        
        # Storage-based calculation
        available_storage_gb = current_resources["storage"]["free_gb"]
        storage_per_sample_gb = 0.15  # Estimated from your data
        
        # Calculate max samples that fit in available storage
        max_samples_by_storage = int(available_storage_gb * strategy_config["storage_threshold"] / storage_per_sample_gb)
        
        # Queue-based calculation
        queue_capacity = self.queue_manager.get_total_queue_capacity()
        max_samples_by_queue = queue_capacity * strategy_config["max_chunk_size"]
        
        # CPU-based calculation
        available_cpus = current_resources["cpu"]["available"]
        max_samples_by_cpu = available_cpus * 20  # Assume 20 samples per CPU
        
        # Cost-based calculation
        optimal_cost_chunk = self.cost_optimizer.calculate_cost_optimal_chunk_size(total_samples)
        
        # Predictive calculation
        predicted_optimal = self.predictor.predict_optimal_chunk_size(total_samples, strategy)
        
        # Combine all factors
        optimal_samples = min(
            max_samples_by_storage,
            max_samples_by_queue,
            max_samples_by_cpu,
            optimal_cost_chunk,
            predicted_optimal,
            strategy_config["max_chunk_size"]
        )
        
        # Ensure minimum chunk size
        min_chunk_size = 100
        optimal_samples = max(optimal_samples, min_chunk_size)
        
        logger.info(f"Optimal chunk size calculated: {optimal_samples} samples (strategy: {strategy})")
        return optimal_samples
        
    def allocate_resources(self, job_requirements: JobRequirements) -> ResourceAllocation:
        """Allocate optimal resources for a job"""
        # Get current system state
        current_resources = self.get_current_resources()
        
        # Calculate optimal allocation
        optimal_allocation = self._calculate_optimal_allocation(job_requirements, current_resources)
        
        # Validate allocation
        validated_allocation = self._validate_allocation(optimal_allocation, current_resources)
        
        # Store in history
        self._record_allocation(job_requirements, validated_allocation)
        
        return validated_allocation
        
    def _calculate_optimal_allocation(self, requirements: JobRequirements, resources: Dict) -> ResourceAllocation:
        """Calculate optimal resource allocation"""
        # Get available queues
        available_queues = self.queue_manager.get_available_queues(requirements)
        
        best_allocation = None
        best_score = float('inf')
        
        for queue_name in available_queues:
            queue_config = self.config["hpc_environment"]["queues"][queue_name]
            
            # Calculate optimal resources for this queue
            cpus = min(requirements.cpus, queue_config["max_cpu_per_job"])
            memory_gb = min(requirements.memory_gb, queue_config["max_memory_per_job"])
            walltime_hours = min(requirements.walltime_hours, queue_config["max_walltime_hours"])
            
            # Estimate cost
            estimated_cost = self.cost_optimizer.estimate_job_cost(cpus, memory_gb, walltime_hours, queue_name)
            
            # Estimate duration
            estimated_duration = self.predictor.predict_job_duration(requirements, cpus, memory_gb, queue_name)
            
            # Calculate success probability
            success_prob = self.predictor.predict_job_success_probability(requirements, cpus, memory_gb, queue_name)
            
            # Calculate score (lower is better)
            cost_weight = self.config["optimization"]["cost_optimization"]["cost_weight"]
            time_weight = self.config["optimization"]["cost_optimization"]["time_weight"]
            reliability_weight = self.config["optimization"]["cost_optimization"]["reliability_weight"]
            
            score = (cost_weight * estimated_cost + 
                    time_weight * estimated_duration + 
                    reliability_weight * (1 - success_prob))
            
            if score < best_score:
                best_score = score
                best_allocation = ResourceAllocation(
                    cpus=cpus,
                    memory_gb=memory_gb,
                    walltime_hours=walltime_hours,
                    queue=queue_name,
                    estimated_cost=estimated_cost,
                    estimated_duration_hours=estimated_duration,
                    success_probability=success_prob
                )
                
        return best_allocation
        
    def _validate_allocation(self, allocation: ResourceAllocation, resources: Dict) -> ResourceAllocation:
        """Validate and adjust resource allocation"""
        # Check if allocation is feasible
        if allocation.estimated_cost > self.config["optimization"]["cost_optimization"]["max_cost_per_job"]:
            logger.warning(f"Job cost {allocation.estimated_cost} exceeds maximum {self.config['optimization']['cost_optimization']['max_cost_per_job']}")
            # Adjust to reduce cost
            allocation.cpus = max(1, allocation.cpus // 2)
            allocation.memory_gb = max(8, allocation.memory_gb // 2)
            allocation.estimated_cost = self.cost_optimizer.estimate_job_cost(
                allocation.cpus, allocation.memory_gb, allocation.walltime_hours, allocation.queue)
                
        return allocation
        
    def _record_allocation(self, requirements: JobRequirements, allocation: ResourceAllocation):
        """Record allocation in history"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "requirements": {
                "cpus": requirements.cpus,
                "memory_gb": requirements.memory_gb,
                "walltime_hours": requirements.walltime_hours,
                "storage_gb": requirements.storage_gb,
                "priority": requirements.priority
            },
            "allocation": {
                "cpus": allocation.cpus,
                "memory_gb": allocation.memory_gb,
                "walltime_hours": allocation.walltime_hours,
                "queue": allocation.queue,
                "estimated_cost": allocation.estimated_cost,
                "estimated_duration_hours": allocation.estimated_duration_hours,
                "success_probability": allocation.success_probability
            }
        }
        
        self.history["job_history"].append(record)
        if len(self.history["job_history"]) > 1000:
            self.history["job_history"] = self.history["job_history"][-1000:]
            
    def _monitor_resources(self):
        """Real-time resource monitoring thread"""
        while True:
            try:
                current_resources = self.get_current_resources()
                if current_resources:
                    # Check for alerts
                    self._check_alerts(current_resources)
                    
                time.sleep(self.config["monitoring"]["update_interval_seconds"])
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(60)  # Wait longer on error
                
    def _check_alerts(self, resources: Dict):
        """Check for resource alerts"""
        alerts = self.config["monitoring"]["alert_thresholds"]
        
        # Storage alert
        if resources["storage"]["usage_percent"] > alerts["storage_usage"] * 100:
            logger.warning(f"Storage usage critical: {resources['storage']['usage_percent']:.1f}%")
            
        # Queue wait time alert
        for queue_name, queue_info in resources["lsf"].items():
            if queue_info["pending"] > alerts["queue_wait_time"]:
                logger.warning(f"Queue {queue_name} has {queue_info['pending']} pending jobs")
                
    def generate_advanced_report(self) -> str:
        """Generate comprehensive resource report"""
        current_resources = self.get_current_resources()
        if not current_resources:
            return "❌ Could not determine system resources"
            
        report = []
        report.append("🧠 ADVANCED HPC RESOURCE REPORT")
        report.append("=" * 60)
        report.append(f"⏰ Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # System Resources
        report.append("🖥️  SYSTEM RESOURCES:")
        report.append(f"  CPU: {current_resources['cpu']['count']} total, {current_resources['cpu']['usage_percent']:.1f}% used")
        report.append(f"  Memory: {current_resources['memory']['total_gb']:.1f}GB total, {current_resources['memory']['usage_percent']:.1f}% used")
        report.append(f"  Storage: {current_resources['storage']['total_gb']:.1f}GB total, {current_resources['storage']['usage_percent']:.1f}% used")
        report.append("")
        
        # Queue Analysis
        report.append("📋 QUEUE ANALYSIS:")
        for queue_name, queue_info in current_resources["lsf"].items():
            queue_config = self.config["hpc_environment"]["queues"].get(queue_name, {})
            utilization = (queue_info["running"] / queue_config.get("max_jobs", 1)) * 100
            report.append(f"  {queue_name}:")
            report.append(f"    Running: {queue_info['running']}/{queue_config.get('max_jobs', 'N/A')} ({utilization:.1f}%)")
            report.append(f"    Pending: {queue_info['pending']}")
            report.append(f"    Cost: ${queue_config.get('cost_per_cpu_hour', 0):.2f}/CPU-hour")
        report.append("")
        
        # Optimization Recommendations
        report.append("🎯 OPTIMIZATION RECOMMENDATIONS:")
        recommendations = self._generate_recommendations(current_resources)
        for rec in recommendations:
            report.append(f"  • {rec}")
        report.append("")
        
        # Cost Analysis
        report.append("💰 COST ANALYSIS:")
        cost_analysis = self.cost_optimizer.analyze_costs()
        for analysis in cost_analysis:
            report.append(f"  {analysis}")
        report.append("")
        
        # Predictive Insights
        report.append("🔮 PREDICTIVE INSIGHTS:")
        insights = self.predictor.generate_insights()
        for insight in insights:
            report.append(f"  {insight}")
            
        return "\n".join(report)
        
    def _generate_recommendations(self, resources: Dict) -> List[str]:
        """Generate optimization recommendations"""
        recommendations = []
        
        # Storage recommendations
        if resources["storage"]["usage_percent"] > 90:
            recommendations.append("Storage usage critical - consider cleanup or expansion")
            
        # Queue recommendations
        for queue_name, queue_info in resources["lsf"].items():
            if queue_info["pending"] > 50:
                recommendations.append(f"Queue {queue_name} overloaded - consider using alternative queues")
                
        # Cost recommendations
        if len(self.history["cost_history"]) > 10:
            avg_cost = np.mean([c["cost"] for c in self.history["cost_history"][-10:]])
            if avg_cost > 50:
                recommendations.append("Average job cost high - consider optimizing resource allocation")
                
        return recommendations
    
    def predict_storage_failure(self, path: str = None) -> Dict:
        """Predict storage failure for the given path"""
        if path is None:
            path = str(self.workflow_dir)
        
        # Get current storage usage
        try:
            statvfs = os.statvfs(path)
            total_space = statvfs.f_frsize * statvfs.f_blocks
            free_space = statvfs.f_frsize * statvfs.f_bavail
            used_space = total_space - free_space
            usage_percent = used_space / total_space
            
            # Estimate growth rate (simplified)
            growth_rate = 0.05  # 5% per hour estimate
            
            return self.storage_predictor.predict_storage_failure(usage_percent, growth_rate)
        except Exception as e:
            logger.error(f"Error predicting storage failure: {e}")
            return {"error": str(e)}
    
    def predict_resource_exhaustion(self) -> Dict:
        """Predict resource exhaustion across all resource types"""
        try:
            # Get current resource usage
            current_resources = self.get_current_resources()
            
            # Format for the exhaustion predictor
            formatted_resources = {
                'cpu': {
                    'usage_percent': current_resources['cpu']['usage_percent'],
                    'available': current_resources['cpu']['available']
                },
                'memory': {
                    'usage_percent': current_resources['memory']['usage_percent'],
                    'available_gb': current_resources['memory']['available_gb']
                },
                'storage': {
                    'usage_percent': current_resources['storage']['usage_percent'],
                    'free_gb': current_resources['storage']['free_gb']
                },
                'lsf': current_resources['lsf']
            }
            
            return self.exhaustion_predictor.predict_resource_exhaustion(formatted_resources)
        except Exception as e:
            logger.error(f"Error predicting resource exhaustion: {e}")
            return {"error": str(e)}
    
    def get_network_topology(self) -> Dict:
        """Get network topology information"""
        try:
            return self.network_topology.discover_network_topology()
        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            return {"error": str(e)}
    
    def get_hpc_system_info(self) -> Dict:
        """Get HPC system information and integration status"""
        try:
            return self.hpc_integration.get_scheduler_info()
        except Exception as e:
            logger.error(f"Error getting HPC system info: {e}")
            return {"error": str(e)}
    
    def optimize_for_topology(self, job_requirements: JobRequirements) -> Dict:
        """Optimize job requirements based on network topology"""
        try:
            topology = self.get_network_topology()
            job_req_dict = {
                'cpus': job_requirements.cpus,
                'memory_gb': job_requirements.memory_gb,
                'storage_gb': job_requirements.storage_gb,
                'walltime_hours': job_requirements.walltime_hours
            }
            return self.network_topology.optimize_for_topology(job_req_dict)
        except Exception as e:
            logger.error(f"Error optimizing for topology: {e}")
            return {"error": str(e)}

class AdvancedQueueManager:
    """Advanced queue management with intelligent selection"""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def get_available_queues(self, requirements: JobRequirements) -> List[str]:
        """Get available queues for job requirements"""
        available_queues = []
        
        for queue_name, queue_config in self.config["hpc_environment"]["queues"].items():
            if (requirements.cpus <= queue_config["max_cpu_per_job"] and
                requirements.memory_gb <= queue_config["max_memory_per_job"] and
                requirements.walltime_hours <= queue_config["max_walltime_hours"]):
                available_queues.append(queue_name)
                
        return available_queues
        
    def get_total_queue_capacity(self) -> int:
        """Get total queue capacity"""
        total_capacity = 0
        for queue_config in self.config["hpc_environment"]["queues"].values():
            total_capacity += queue_config["max_jobs"]
        return total_capacity

class ResourcePredictor:
    """ML-based resource prediction"""
    
    def __init__(self, history: Dict):
        self.history = history
        
    def predict_optimal_chunk_size(self, total_samples: int, strategy: str) -> int:
        """Predict optimal chunk size using historical data"""
        if not self.history["job_history"]:
            return 1000  # Default
            
        # Simple prediction based on historical performance
        recent_jobs = self.history["job_history"][-100:]  # Last 100 jobs
        
        # Calculate average chunk size for similar jobs
        similar_jobs = [job for job in recent_jobs if job["requirements"]["storage_gb"] > 0]
        if similar_jobs:
            avg_chunk_size = np.mean([job["allocation"]["cpus"] * 100 for job in similar_jobs])
            return int(avg_chunk_size)
            
        return 1000
        
    def predict_job_duration(self, requirements: JobRequirements, cpus: int, memory_gb: int, queue: str) -> float:
        """Predict job duration"""
        # Simple prediction based on resource requirements
        base_duration = requirements.storage_gb / (cpus * 0.1)  # Rough estimate
        queue_factor = self._get_queue_speed_factor(queue)
        return base_duration * queue_factor
        
    def predict_job_success_probability(self, requirements: JobRequirements, cpus: int, memory_gb: int, queue: str) -> float:
        """Predict job success probability"""
        # Simple prediction based on resource adequacy
        cpu_adequacy = min(1.0, cpus / requirements.cpus)
        memory_adequacy = min(1.0, memory_gb / requirements.memory_gb)
        
        base_probability = (cpu_adequacy + memory_adequacy) / 2
        
        # Adjust based on queue reliability
        queue_reliability = self._get_queue_reliability(queue)
        
        return base_probability * queue_reliability
        
    def _get_queue_speed_factor(self, queue: str) -> float:
        """Get queue speed factor"""
        queue_factors = {
            "normal": 1.0,
            "hiprio": 0.7,
            "long": 1.5,
            "gpu": 0.5
        }
        return queue_factors.get(queue, 1.0)
        
    def _get_queue_reliability(self, queue: str) -> float:
        """Get queue reliability factor"""
        reliability_factors = {
            "normal": 0.95,
            "hiprio": 0.98,
            "long": 0.90,
            "gpu": 0.85
        }
        return reliability_factors.get(queue, 0.95)
        
    def generate_insights(self) -> List[str]:
        """Generate predictive insights"""
        insights = []
        
        if len(self.history["job_history"]) > 10:
            recent_jobs = self.history["job_history"][-10:]
            avg_success_rate = np.mean([job["allocation"]["success_probability"] for job in recent_jobs])
            insights.append(f"Recent job success rate: {avg_success_rate:.1%}")
            
            avg_cost = np.mean([job["allocation"]["estimated_cost"] for job in recent_jobs])
            insights.append(f"Average job cost: ${avg_cost:.2f}")
            
        return insights

class CostOptimizer:
    """Cost optimization for resource allocation"""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def estimate_job_cost(self, cpus: int, memory_gb: int, walltime_hours: float, queue: str) -> float:
        """Estimate job cost"""
        queue_config = self.config["hpc_environment"]["queues"].get(queue, {})
        cost_per_cpu_hour = queue_config.get("cost_per_cpu_hour", 0.1)
        
        # Base cost calculation
        base_cost = cpus * walltime_hours * cost_per_cpu_hour
        
        # Memory cost factor (memory is typically more expensive)
        memory_factor = 1 + (memory_gb / 128) * 0.1
        
        return base_cost * memory_factor
        
    def calculate_cost_optimal_chunk_size(self, total_samples: int) -> int:
        """Calculate cost-optimal chunk size"""
        # Simple cost optimization
        # Larger chunks = fewer jobs = lower overhead
        # But larger chunks = higher resource requirements = higher cost per job
        
        # Find sweet spot
        optimal_chunk_size = min(2000, max(500, total_samples // 20))
        return optimal_chunk_size
        
    def analyze_costs(self) -> List[str]:
        """Analyze cost patterns"""
        analysis = []
        
        if len(self.config["hpc_environment"]["queues"]) > 1:
            analysis.append("Queue cost comparison:")
            for queue_name, queue_config in self.config["hpc_environment"]["queues"].items():
                cost = queue_config.get("cost_per_cpu_hour", 0)
                analysis.append(f"  {queue_name}: ${cost:.2f}/CPU-hour")
                
        return analysis

class WorkloadBalancer:
    """Workload balancing and optimization"""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def balance_workload(self, pending_jobs: List[JobRequirements]) -> List[JobRequirements]:
        """Balance workload across available resources"""
        # Simple workload balancing
        # Sort by priority and resource requirements
        balanced_jobs = sorted(pending_jobs, key=lambda x: (x.priority, x.cpus, x.memory_gb))
        return balanced_jobs

def main():
    """Test the advanced resource manager"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Advanced HPC Resource Manager')
    parser.add_argument('--workflow-dir', default='.', help='Workflow directory')
    parser.add_argument('--report', action='store_true', help='Generate advanced report')
    parser.add_argument('--test-allocation', action='store_true', help='Test resource allocation')
    
    args = parser.parse_args()
    
    manager = AdvancedResourceManager(Path(args.workflow_dir))
    
    if args.report:
        print(manager.generate_advanced_report())
    elif args.test_allocation:
        # Test resource allocation
        test_requirements = JobRequirements(
            cpus=8,
            memory_gb=128,
            walltime_hours=72,
            storage_gb=5.0,
            priority=1
        )
        
        allocation = manager.allocate_resources(test_requirements)
        print(f"Optimal allocation:")
        print(f"  CPUs: {allocation.cpus}")
        print(f"  Memory: {allocation.memory_gb}GB")
        print(f"  Walltime: {allocation.walltime_hours}h")
        print(f"  Queue: {allocation.queue}")
        print(f"  Estimated cost: ${allocation.estimated_cost:.2f}")
        print(f"  Success probability: {allocation.success_probability:.1%}")
    else:
        print("Use --help to see available options")

if __name__ == '__main__':
    main()