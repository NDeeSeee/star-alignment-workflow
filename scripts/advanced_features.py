#!/usr/bin/env python3
"""
Advanced HPC Features Module
Storage failure prediction, resource exhaustion prediction, and network topology awareness
"""

import os
import json
import subprocess
import psutil
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque
import threading
import time
import logging
from typing import Dict, List, Optional, Tuple
import socket
import platform

logger = logging.getLogger(__name__)

class StorageFailurePredictor:
    """Predict storage failures and provide preventive measures"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.storage_history = deque(maxlen=1000)
        self.failure_thresholds = {
            "usage_critical": 0.95,
            "usage_warning": 0.90,
            "growth_rate_critical": 0.1,  # 10% growth per hour
            "io_error_threshold": 5,      # 5 IO errors per hour
            "disk_health_warning": 0.8     # 80% disk health
        }
        
    def predict_storage_failure(self, current_usage: float, growth_rate: float = 0.0) -> Dict:
        """Predict storage failure probability and timeline"""
        try:
            # Validate and convert inputs
            try:
                if isinstance(current_usage, str):
                    current_usage = float(current_usage)
                elif not isinstance(current_usage, (int, float)):
                    current_usage = 0.0
                current_usage = max(0.0, min(1.0, float(current_usage)))  # Clamp to 0-1
            except (ValueError, TypeError):
                current_usage = 0.0
            
            try:
                if isinstance(growth_rate, str):
                    growth_rate = float(growth_rate)
                elif not isinstance(growth_rate, (int, float)):
                    growth_rate = 0.0
                growth_rate = max(0.0, float(growth_rate))  # Ensure non-negative
            except (ValueError, TypeError):
                growth_rate = 0.0
            
            # Calculate failure probability
            failure_probability = self._calculate_failure_probability(current_usage, growth_rate)
            
            # Estimate time to failure
            time_to_failure = self._estimate_time_to_failure(current_usage, growth_rate)
            
            # Get preventive measures
            preventive_measures = self._get_preventive_measures(current_usage, failure_probability)
            
            # Check disk health
            disk_health = self._check_disk_health()
            
            return {
                "failure_probability": failure_probability,
                "time_to_failure_hours": time_to_failure,
                "risk_level": self._get_risk_level(failure_probability),
                "preventive_measures": preventive_measures,
                "disk_health": disk_health,
                "recommendations": self._get_recommendations(failure_probability, time_to_failure)
            }
        except Exception as e:
            logger.error(f"Error in predict_storage_failure: {e}")
            return {
                "error": str(e),
                "failure_probability": 0.0,
                "time_to_failure_hours": 0.0,
                "risk_level": "UNKNOWN",
                "preventive_measures": ["Error occurred during prediction"],
                "disk_health": {"status": "unknown", "recent_errors": 0, "health_score": 0.0},
                "recommendations": ["Error occurred during prediction"]
            }
        
    def _calculate_failure_probability(self, usage: float, growth_rate: float) -> float:
        """Calculate storage failure probability"""
        base_probability = 0.0
        
        # Usage-based probability
        if usage > self.failure_thresholds["usage_critical"]:
            base_probability += 0.7
        elif usage > self.failure_thresholds["usage_warning"]:
            base_probability += 0.3
            
        # Growth rate-based probability
        if growth_rate > self.failure_thresholds["growth_rate_critical"]:
            base_probability += 0.4
            
        # IO error-based probability
        io_errors = self._get_io_error_rate()
        if io_errors > self.failure_thresholds["io_error_threshold"]:
            base_probability += 0.2
            
        return min(base_probability, 1.0)
        
    def _estimate_time_to_failure(self, usage: float, growth_rate: float) -> float:
        """Estimate time to storage failure in hours"""
        if growth_rate <= 0:
            return float('inf')  # No growth, no immediate failure
            
        # Calculate time to reach 100% usage
        remaining_capacity = 1.0 - usage
        time_to_full = remaining_capacity / growth_rate
        
        # Add safety margin (fail at 98% usage)
        safety_margin = 0.02
        time_to_failure = (remaining_capacity - safety_margin) / growth_rate
        
        return max(0, time_to_failure)
        
    def _get_preventive_measures(self, usage: float, failure_prob: float) -> List[str]:
        """Get preventive measures based on current state"""
        measures = []
        
        if failure_prob > 0.8:
            measures.extend([
                "🚨 CRITICAL: Immediate storage cleanup required",
                "🚨 CRITICAL: Request storage expansion immediately",
                "🚨 CRITICAL: Stop new job submissions",
                "🚨 CRITICAL: Archive old data to external storage"
            ])
        elif failure_prob > 0.5:
            measures.extend([
                "⚠️  HIGH RISK: Clean up temporary files",
                "⚠️  HIGH RISK: Compress old BAM files",
                "⚠️  HIGH RISK: Request additional storage",
                "⚠️  HIGH RISK: Monitor storage usage closely"
            ])
        elif failure_prob > 0.2:
            measures.extend([
                "📊 MODERATE RISK: Regular cleanup schedule",
                "📊 MODERATE RISK: Monitor growth rate",
                "📊 MODERATE RISK: Plan storage expansion"
            ])
        else:
            measures.extend([
                "✅ LOW RISK: Continue normal operations",
                "✅ LOW RISK: Regular monitoring sufficient"
            ])
            
        return measures
        
    def _check_disk_health(self) -> Dict:
        """Check disk health using system tools"""
        try:
            # Check disk health using smartctl if available
            result = subprocess.run(['smartctl', '-H', '/dev/sda'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                health_status = "healthy" if "PASSED" in result.stdout else "warning"
            else:
                health_status = "unknown"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            health_status = "unknown"
            
        # Check for disk errors in system logs
        try:
            result = subprocess.run(['dmesg'], capture_output=True, text=True, timeout=5)
            disk_errors = result.stdout.count('I/O error') + result.stdout.count('disk error')
        except:
            disk_errors = 0
            
        return {
            "status": health_status,
            "recent_errors": disk_errors,
            "health_score": max(0, 1.0 - (disk_errors * 0.1))
        }
        
    def _get_io_error_rate(self) -> int:
        """Get current IO error rate"""
        try:
            result = subprocess.run(['iostat', '-x', '1', '1'], 
                                  capture_output=True, text=True, timeout=10)
            # Parse iostat output for error rates
            lines = result.stdout.split('\n')
            for line in lines:
                if 'sda' in line or 'nvme' in line:
                    parts = line.split()
                    if len(parts) > 10:
                        return int(parts[9])  # Error count
        except:
            pass
        return 0
        
    def _get_risk_level(self, failure_prob: float) -> str:
        """Get risk level based on failure probability"""
        if failure_prob > 0.8:
            return "CRITICAL"
        elif failure_prob > 0.5:
            return "HIGH"
        elif failure_prob > 0.2:
            return "MODERATE"
        else:
            return "LOW"
            
    def _get_recommendations(self, failure_prob: float, time_to_failure: float) -> List[str]:
        """Get specific recommendations"""
        recommendations = []
        
        if time_to_failure < 24:  # Less than 24 hours
            recommendations.append("🚨 URGENT: Storage will fail within 24 hours")
            recommendations.append("🚨 URGENT: Implement emergency cleanup procedures")
        elif time_to_failure < 168:  # Less than 1 week
            recommendations.append("⚠️  WARNING: Storage will fail within 1 week")
            recommendations.append("⚠️  WARNING: Plan storage expansion immediately")
        elif time_to_failure < 720:  # Less than 1 month
            recommendations.append("📊 NOTICE: Storage will fail within 1 month")
            recommendations.append("📊 NOTICE: Schedule storage maintenance")
        else:
            recommendations.append("✅ OK: Storage stable for extended period")
            
        return recommendations

class ResourceExhaustionPredictor:
    """Predict resource exhaustion and provide optimization recommendations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.resource_history = deque(maxlen=1000)
        self.exhaustion_thresholds = {
            "cpu_critical": 0.95,
            "memory_critical": 0.95,
            "storage_critical": 0.95,
            "queue_critical": 0.9,
            "growth_rate_critical": 0.1
        }
        
    def predict_resource_exhaustion(self, current_resources: Dict) -> Dict:
        """Predict resource exhaustion across all resource types"""
        
        try:
            # Validate input
            if current_resources is None:
                return {
                    "error": "No resource data provided",
                    "predictions": {},
                    "overall_risk": {"overall_risk_level": "UNKNOWN"},
                    "recommendations": ["No resource data available"],
                    "optimization_suggestions": []
                }
            
            if not isinstance(current_resources, dict):
                return {
                    "error": "Invalid resource data format",
                    "predictions": {},
                    "overall_risk": {"overall_risk_level": "UNKNOWN"},
                    "recommendations": ["Invalid resource data format"],
                    "optimization_suggestions": []
                }
            
            predictions = {}
            
            # CPU exhaustion prediction
            if "cpu" in current_resources and current_resources["cpu"] is not None:
                predictions["cpu"] = self._predict_cpu_exhaustion(current_resources["cpu"])
            else:
                predictions["cpu"] = {"error": "CPU data not available"}
            
            # Memory exhaustion prediction
            if "memory" in current_resources and current_resources["memory"] is not None:
                predictions["memory"] = self._predict_memory_exhaustion(current_resources["memory"])
            else:
                predictions["memory"] = {"error": "Memory data not available"}
            
            # Storage exhaustion prediction
            if "storage" in current_resources and current_resources["storage"] is not None:
                predictions["storage"] = self._predict_storage_exhaustion(current_resources["storage"])
            else:
                predictions["storage"] = {"error": "Storage data not available"}
            
            # Queue exhaustion prediction
            if "lsf" in current_resources and current_resources["lsf"] is not None:
                predictions["queues"] = self._predict_queue_exhaustion(current_resources["lsf"])
            else:
                predictions["queues"] = {"error": "Queue data not available"}
            
            # Overall risk assessment
            overall_risk = self._assess_overall_risk(predictions)
            
            return {
                "predictions": predictions,
                "overall_risk": overall_risk,
                "recommendations": self._get_exhaustion_recommendations(predictions, overall_risk),
                "optimization_suggestions": self._get_optimization_suggestions(predictions)
            }
        except Exception as e:
            logger.error(f"Error in predict_resource_exhaustion: {e}")
            return {
                "error": str(e),
                "predictions": {},
                "overall_risk": {"overall_risk_level": "UNKNOWN"},
                "recommendations": ["Error occurred during prediction"],
                "optimization_suggestions": []
            }
        
    def _predict_cpu_exhaustion(self, cpu_info: Dict) -> Dict:
        """Predict CPU resource exhaustion"""
        try:
            # Validate input
            if cpu_info is None or not isinstance(cpu_info, dict):
                return {
                    "error": "Invalid CPU data format",
                    "exhaustion_probability": 0.0,
                    "time_to_exhaustion_hours": 0.0,
                    "current_usage": 0.0,
                    "available_cores": 0,
                    "risk_level": "UNKNOWN"
                }
            
            # Safely extract values with type validation
            usage_percent = cpu_info.get("usage_percent", 0)
            available = cpu_info.get("available", 1)
            
            # Validate and convert usage_percent
            try:
                if isinstance(usage_percent, str):
                    usage_percent = float(usage_percent)
                elif not isinstance(usage_percent, (int, float)):
                    usage_percent = 0
                usage = max(0, min(100, usage_percent)) / 100  # Clamp to 0-100%
            except (ValueError, TypeError):
                usage = 0
            
            # Validate and convert available
            try:
                if isinstance(available, str):
                    available = int(available)
                elif not isinstance(available, (int, float)):
                    available = 1
                available = max(0, int(available))
            except (ValueError, TypeError):
                available = 1
            
            # Calculate exhaustion probability
            exhaustion_prob = 0.0
            if usage > self.exhaustion_thresholds["cpu_critical"]:
                exhaustion_prob = 0.8
            elif usage > 0.8:
                exhaustion_prob = 0.4
            elif usage > 0.6:
                exhaustion_prob = 0.1
                
            # Estimate time to exhaustion
            time_to_exhaustion = self._estimate_cpu_exhaustion_time(usage, available)
            
            return {
                "exhaustion_probability": exhaustion_prob,
                "time_to_exhaustion_hours": time_to_exhaustion,
                "current_usage": usage,
                "available_cores": available,
                "risk_level": self._get_risk_level(exhaustion_prob)
            }
        except Exception as e:
            logger.error(f"Error in _predict_cpu_exhaustion: {e}")
            return {
                "error": str(e),
                "exhaustion_probability": 0.0,
                "time_to_exhaustion_hours": 0.0,
                "current_usage": 0.0,
                "available_cores": 0,
                "risk_level": "UNKNOWN"
            }
        
    def _predict_memory_exhaustion(self, memory_info: Dict) -> Dict:
        """Predict memory resource exhaustion"""
        try:
            # Validate input
            if memory_info is None or not isinstance(memory_info, dict):
                return {
                    "error": "Invalid memory data format",
                    "exhaustion_probability": 0.0,
                    "time_to_exhaustion_hours": 0.0,
                    "current_usage": 0.0,
                    "available_gb": 0,
                    "risk_level": "UNKNOWN"
                }
            
            # Safely extract values with type validation
            usage_percent = memory_info.get("usage_percent", 0)
            available_gb = memory_info.get("available_gb", 1)
            
            # Validate and convert usage_percent
            try:
                if isinstance(usage_percent, str):
                    usage_percent = float(usage_percent)
                elif not isinstance(usage_percent, (int, float)):
                    usage_percent = 0
                usage = max(0, min(100, usage_percent)) / 100  # Clamp to 0-100%
            except (ValueError, TypeError):
                usage = 0
            
            # Validate and convert available_gb
            try:
                if isinstance(available_gb, str):
                    available_gb = float(available_gb)
                elif not isinstance(available_gb, (int, float)):
                    available_gb = 1
                available_gb = max(0, float(available_gb))
            except (ValueError, TypeError):
                available_gb = 1
        
            # Calculate exhaustion probability
            exhaustion_prob = 0.0
            if usage > self.exhaustion_thresholds["memory_critical"]:
                exhaustion_prob = 0.9
            elif usage > 0.8:
                exhaustion_prob = 0.5
            elif usage > 0.6:
                exhaustion_prob = 0.2
                
            # Estimate time to exhaustion
            time_to_exhaustion = self._estimate_memory_exhaustion_time(usage, available_gb)
            
            return {
                "exhaustion_probability": exhaustion_prob,
                "time_to_exhaustion_hours": time_to_exhaustion,
                "current_usage": usage,
                "available_gb": available_gb,
                "risk_level": self._get_risk_level(exhaustion_prob)
            }
        except Exception as e:
            logger.error(f"Error in _predict_memory_exhaustion: {e}")
            return {
                "error": str(e),
                "exhaustion_probability": 0.0,
                "time_to_exhaustion_hours": 0.0,
                "current_usage": 0.0,
                "available_gb": 0,
                "risk_level": "UNKNOWN"
            }
        
    def _predict_storage_exhaustion(self, storage_info: Dict) -> Dict:
        """Predict storage resource exhaustion"""
        try:
            # Validate input
            if storage_info is None or not isinstance(storage_info, dict):
                return {
                    "error": "Invalid storage data format",
                    "exhaustion_probability": 0.0,
                    "time_to_exhaustion_hours": 0.0,
                    "current_usage": 0.0,
                    "free_gb": 0,
                    "risk_level": "UNKNOWN"
                }
            
            # Safely extract values with type validation
            usage_percent = storage_info.get("usage_percent", 0)
            free_gb = storage_info.get("free_gb", 1)
            
            # Validate and convert usage_percent
            try:
                if isinstance(usage_percent, str):
                    usage_percent = float(usage_percent)
                elif not isinstance(usage_percent, (int, float)):
                    usage_percent = 0
                usage = max(0, min(100, usage_percent)) / 100  # Clamp to 0-100%
            except (ValueError, TypeError):
                usage = 0
            
            # Validate and convert free_gb
            try:
                if isinstance(free_gb, str):
                    free_gb = float(free_gb)
                elif not isinstance(free_gb, (int, float)):
                    free_gb = 1
                free_gb = max(0, float(free_gb))
            except (ValueError, TypeError):
                free_gb = 1
        
            # Calculate exhaustion probability
            exhaustion_prob = 0.0
            if usage > self.exhaustion_thresholds["storage_critical"]:
                exhaustion_prob = 0.95
            elif usage > 0.9:
                exhaustion_prob = 0.7
            elif usage > 0.8:
                exhaustion_prob = 0.3
                
            # Estimate time to exhaustion
            time_to_exhaustion = self._estimate_storage_exhaustion_time(usage, free_gb)
            
            return {
                "exhaustion_probability": exhaustion_prob,
                "time_to_exhaustion_hours": time_to_exhaustion,
                "current_usage": usage,
                "free_gb": free_gb,
                "risk_level": self._get_risk_level(exhaustion_prob)
            }
        except Exception as e:
            logger.error(f"Error in _predict_storage_exhaustion: {e}")
            return {
                "error": str(e),
                "exhaustion_probability": 0.0,
                "time_to_exhaustion_hours": 0.0,
                "current_usage": 0.0,
                "free_gb": 0,
                "risk_level": "UNKNOWN"
            }
        
    def _predict_queue_exhaustion(self, queue_info: Dict) -> Dict:
        """Predict queue resource exhaustion"""
        try:
            # Validate input
            if queue_info is None or not isinstance(queue_info, dict):
                return {"error": "Invalid queue data format"}
            
            queue_exhaustion = {}
            
            for queue_name, queue_data in queue_info.items():
                try:
                    # Validate queue data
                    if queue_data is None or not isinstance(queue_data, dict):
                        queue_exhaustion[queue_name] = {"error": "Invalid queue data"}
                        continue
                    
                    # Safely extract values with defaults
                    pending = queue_data.get("pending", 0)
                    running = queue_data.get("running", 0)
                    max_jobs = queue_data.get("max_jobs", 100)
                    
                    # Validate and convert values
                    try:
                        pending = int(pending) if isinstance(pending, (int, str)) else 0
                        running = int(running) if isinstance(running, (int, str)) else 0
                        max_jobs = int(max_jobs) if isinstance(max_jobs, (int, str)) else 100
                    except (ValueError, TypeError):
                        pending = running = 0
                        max_jobs = 100
                    
                    # Ensure non-negative values
                    pending = max(0, pending)
                    running = max(0, running)
                    max_jobs = max(1, max_jobs)  # At least 1 to avoid division by zero
                    
                    # Calculate queue utilization
                    utilization = (running + pending) / max_jobs
                    
                    # Calculate exhaustion probability
                    exhaustion_prob = 0.0
                    if utilization > self.exhaustion_thresholds["queue_critical"]:
                        exhaustion_prob = 0.8
                    elif utilization > 0.8:
                        exhaustion_prob = 0.4
                    elif utilization > 0.6:
                        exhaustion_prob = 0.1
                        
                    queue_exhaustion[queue_name] = {
                        "exhaustion_probability": exhaustion_prob,
                        "utilization": utilization,
                        "pending_jobs": pending,
                        "running_jobs": running,
                        "max_jobs": max_jobs,
                        "risk_level": self._get_risk_level(exhaustion_prob)
                    }
                except Exception as e:
                    queue_exhaustion[queue_name] = {"error": str(e)}
                    
            return queue_exhaustion
        except Exception as e:
            logger.error(f"Error in _predict_queue_exhaustion: {e}")
            return {"error": str(e)}
        
    def _estimate_cpu_exhaustion_time(self, usage: float, available: int) -> float:
        """Estimate time to CPU exhaustion"""
        if usage >= 0.95:
            return 0.1  # Already critical
        elif available <= 0:
            return 0.5  # No available cores
            
        # Simple linear projection
        remaining_capacity = 0.95 - usage
        if remaining_capacity <= 0:
            return 0.1
            
        # Assume current job load continues
        time_to_exhaustion = remaining_capacity * 24  # Hours
        
        return max(0.1, time_to_exhaustion)
        
    def _estimate_memory_exhaustion_time(self, usage: float, available_gb: float) -> float:
        """Estimate time to memory exhaustion"""
        if usage >= 0.95:
            return 0.1  # Already critical
            
        # Simple linear projection
        remaining_capacity = 0.95 - usage
        if remaining_capacity <= 0:
            return 0.1
            
        # Assume current memory usage pattern continues
        time_to_exhaustion = remaining_capacity * 48  # Hours
        
        return max(0.1, time_to_exhaustion)
        
    def _estimate_storage_exhaustion_time(self, usage: float, free_gb: float) -> float:
        """Estimate time to storage exhaustion"""
        if usage >= 0.95:
            return 0.1  # Already critical
            
        # Simple linear projection based on current usage
        remaining_capacity = 0.95 - usage
        if remaining_capacity <= 0:
            return 0.1
            
        # Assume current storage growth rate continues
        time_to_exhaustion = remaining_capacity * 168  # Hours (1 week)
        
        return max(0.1, time_to_exhaustion)
        
    def _assess_overall_risk(self, predictions: Dict) -> Dict:
        """Assess overall resource exhaustion risk"""
        try:
            risks = []
            
            # Collect all risk levels
            for resource_type, prediction in predictions.items():
                if isinstance(prediction, dict):
                    if "error" in prediction:
                        # Skip resources with errors
                        continue
                    elif "risk_level" in prediction:
                        risks.append(prediction["risk_level"])
                    else:
                        # For queues, get the highest risk
                        queue_risks = [q["risk_level"] for q in prediction.values() 
                                     if isinstance(q, dict) and "risk_level" in q]
                        if queue_risks:
                            risks.extend(queue_risks)
                        
            # Calculate overall risk
            if not risks:
                overall_risk_level = "UNKNOWN"
            else:
                risk_scores = {"LOW": 1, "MODERATE": 2, "HIGH": 3, "CRITICAL": 4, "UNKNOWN": 0}
                max_risk_score = max([risk_scores.get(risk, 1) for risk in risks])
                overall_risk_level = [k for k, v in risk_scores.items() if v == max_risk_score][0]
                
            return {
                "overall_risk_level": overall_risk_level,
                "resource_risks": risks,
                "critical_resources": [r for r in risks if r in ["HIGH", "CRITICAL"]]
            }
        except Exception as e:
            logger.error(f"Error in _assess_overall_risk: {e}")
            return {
                "overall_risk_level": "UNKNOWN",
                "resource_risks": [],
                "critical_resources": []
            }
        
    def _get_exhaustion_recommendations(self, predictions: Dict, overall_risk: Dict) -> List[str]:
        """Get recommendations for resource exhaustion prevention"""
        try:
            recommendations = []
            
            risk_level = overall_risk.get("overall_risk_level", "UNKNOWN")
            
            if risk_level == "CRITICAL":
                recommendations.extend([
                    "🚨 CRITICAL: Immediate resource management required",
                    "🚨 CRITICAL: Reduce job concurrency",
                    "🚨 CRITICAL: Implement emergency resource allocation",
                    "🚨 CRITICAL: Contact system administrators"
                ])
            elif risk_level == "HIGH":
                recommendations.extend([
                    "⚠️  HIGH RISK: Optimize resource allocation",
                    "⚠️  HIGH RISK: Reduce job queue size",
                    "⚠️  HIGH RISK: Monitor resource usage closely",
                    "⚠️  HIGH RISK: Plan resource expansion"
                ])
            elif risk_level == "MODERATE":
                recommendations.extend([
                    "📊 MODERATE RISK: Continue monitoring",
                    "📊 MODERATE RISK: Optimize job scheduling",
                    "📊 MODERATE RISK: Plan resource management"
                ])
            elif risk_level == "UNKNOWN":
                recommendations.extend([
                    "❓ UNKNOWN RISK: Check system status",
                    "❓ UNKNOWN RISK: Verify resource data"
                ])
            else:
                recommendations.extend([
                    "✅ LOW RISK: Normal operations",
                    "✅ LOW RISK: Regular monitoring sufficient"
                ])
                
            return recommendations
        except Exception as e:
            logger.error(f"Error in _get_exhaustion_recommendations: {e}")
            return ["Error occurred while generating recommendations"]
        
    def _get_optimization_suggestions(self, predictions: Dict) -> List[str]:
        """Get specific optimization suggestions"""
        try:
            suggestions = []
            
            # CPU optimization
            if "cpu" in predictions and isinstance(predictions["cpu"], dict):
                cpu_pred = predictions["cpu"]
                if "error" not in cpu_pred and cpu_pred.get("exhaustion_probability", 0) > 0.3:
                    suggestions.append("🖥️  CPU: Reduce job concurrency or increase CPU allocation")
                    
            # Memory optimization
            if "memory" in predictions and isinstance(predictions["memory"], dict):
                memory_pred = predictions["memory"]
                if "error" not in memory_pred and memory_pred.get("exhaustion_probability", 0) > 0.3:
                    suggestions.append("🧠 Memory: Optimize memory usage or increase memory allocation")
                    
            # Storage optimization
            if "storage" in predictions and isinstance(predictions["storage"], dict):
                storage_pred = predictions["storage"]
                if "error" not in storage_pred and storage_pred.get("exhaustion_probability", 0) > 0.3:
                    suggestions.append("💾 Storage: Clean up temporary files or expand storage")
                    
            # Queue optimization
            if "queues" in predictions and isinstance(predictions["queues"], dict):
                for queue_name, queue_pred in predictions["queues"].items():
                    if isinstance(queue_pred, dict) and "error" not in queue_pred:
                        if queue_pred.get("exhaustion_probability", 0) > 0.3:
                            suggestions.append(f"📋 Queue {queue_name}: Reduce pending jobs or use alternative queues")
                            
            return suggestions
        except Exception as e:
            logger.error(f"Error in _get_optimization_suggestions: {e}")
            return ["Error occurred while generating optimization suggestions"]
        
    def _get_risk_level(self, probability: float) -> str:
        """Get risk level based on probability"""
        if probability > 0.8:
            return "CRITICAL"
        elif probability > 0.5:
            return "HIGH"
        elif probability > 0.2:
            return "MODERATE"
        else:
            return "LOW"

class NetworkTopologyAwareness:
    """Network topology awareness for HPC optimization"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.topology_cache = {}
        self.network_performance_history = deque(maxlen=1000)
        
    def discover_network_topology(self) -> Dict:
        """Discover network topology and performance characteristics"""
        
        topology = {
            "host_info": self._get_host_info(),
            "network_interfaces": self._get_network_interfaces(),
            "routing_info": self._get_routing_info(),
            "performance_metrics": self._get_network_performance(),
            "storage_connectivity": self._get_storage_connectivity(),
            "compute_node_connectivity": self._get_compute_node_connectivity()
        }
        
        # Cache topology information
        self.topology_cache = topology
        
        return topology
        
    def _get_host_info(self) -> Dict:
        """Get host information"""
        return {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "architecture": platform.architecture(),
            "processor": platform.processor(),
            "machine": platform.machine()
        }
        
    def _get_network_interfaces(self) -> Dict:
        """Get network interface information"""
        interfaces = {}
        
        try:
            # Get network interface information
            result = subprocess.run(['ip', 'addr', 'show'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                interfaces = self._parse_network_interfaces(result.stdout)
        except:
            # Fallback to basic interface info
            interfaces = {"eth0": {"status": "unknown", "speed": "unknown"}}
            
        return interfaces
        
    def _parse_network_interfaces(self, ip_output: str) -> Dict:
        """Parse ip addr show output"""
        interfaces = {}
        current_interface = None
        
        for line in ip_output.split('\n'):
            if ':' in line and not line.startswith(' '):
                # Interface line
                parts = line.split(':')
                if len(parts) >= 2:
                    interface_name = parts[1].strip()
                    current_interface = interface_name
                    interfaces[interface_name] = {
                        "status": "up" if "UP" in line else "down",
                        "speed": "unknown"
                    }
            elif current_interface and "link/ether" in line:
                # MAC address line
                interfaces[current_interface]["mac"] = line.split()[1]
                
        return interfaces
        
    def _get_routing_info(self) -> Dict:
        """Get routing information"""
        routing_info = {}
        
        try:
            # Get routing table
            result = subprocess.run(['ip', 'route', 'show'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                routing_info["routes"] = result.stdout.split('\n')
                
            # Get default gateway
            result = subprocess.run(['ip', 'route', 'show', 'default'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                routing_info["default_gateway"] = result.stdout.strip()
                
        except:
            routing_info["error"] = "Could not get routing information"
            
        return routing_info
        
    def _get_network_performance(self) -> Dict:
        """Get network performance metrics"""
        performance = {}
        
        try:
            # Get network statistics
            net_stats = psutil.net_io_counters()
            performance = {
                "bytes_sent": net_stats.bytes_sent,
                "bytes_recv": net_stats.bytes_recv,
                "packets_sent": net_stats.packets_sent,
                "packets_recv": net_stats.packets_recv,
                "errin": net_stats.errin,
                "errout": net_stats.errout,
                "dropin": net_stats.dropin,
                "dropout": net_stats.dropout
            }
            
            # Calculate error rates
            total_packets = net_stats.packets_sent + net_stats.packets_recv
            if total_packets > 0:
                performance["error_rate"] = (net_stats.errin + net_stats.errout) / total_packets
                performance["drop_rate"] = (net_stats.dropin + net_stats.dropout) / total_packets
            else:
                performance["error_rate"] = 0
                performance["drop_rate"] = 0
                
        except Exception as e:
            performance["error"] = str(e)
            
        return performance
        
    def _get_storage_connectivity(self) -> Dict:
        """Get storage connectivity information"""
        storage_info = {}
        
        # Check NFS mounts
        try:
            result = subprocess.run(['mount', '-t', 'nfs'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                storage_info["nfs_mounts"] = result.stdout.split('\n')
        except:
            storage_info["nfs_mounts"] = []
            
        # Check local storage
        try:
            result = subprocess.run(['df', '-h'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                storage_info["local_storage"] = result.stdout.split('\n')
        except:
            storage_info["local_storage"] = []
            
        return storage_info
        
    def _get_compute_node_connectivity(self) -> Dict:
        """Get compute node connectivity information"""
        connectivity = {}
        
        # Check if we're on a compute node
        hostname = socket.gethostname()
        connectivity["hostname"] = hostname
        
        # Check for common HPC node patterns
        if any(pattern in hostname.lower() for pattern in ['node', 'compute', 'worker']):
            connectivity["node_type"] = "compute"
        else:
            connectivity["node_type"] = "head/login"
            
        # Check for job scheduler environment
        scheduler_vars = ['SLURM_JOB_ID', 'PBS_JOBID', 'LSB_JOBID']
        for var in scheduler_vars:
            if var in os.environ:
                connectivity["scheduler"] = var.split('_')[0]
                connectivity["job_id"] = os.environ[var]
                break
                
        return connectivity
        
    def optimize_for_topology(self, job_requirements: Dict) -> Dict:
        """Optimize job requirements based on network topology"""
        topology = self.topology_cache or self.discover_network_topology()
        
        optimizations = {
            "data_locality": self._optimize_data_locality(job_requirements, topology),
            "network_awareness": self._optimize_network_awareness(job_requirements, topology),
            "storage_optimization": self._optimize_storage_access(job_requirements, topology)
        }
        
        return optimizations
        
    def _optimize_data_locality(self, job_requirements: Dict, topology: Dict) -> Dict:
        """Optimize for data locality"""
        recommendations = []
        
        # Check if data is local or remote
        if "nfs_mounts" in topology.get("storage_connectivity", {}):
            recommendations.append("📁 Data appears to be on NFS - consider data staging")
        else:
            recommendations.append("📁 Data appears to be local - good for performance")
            
        return {
            "recommendations": recommendations,
            "data_locality_score": 0.8 if "nfs_mounts" not in topology.get("storage_connectivity", {}) else 0.4
        }
        
    def _optimize_network_awareness(self, job_requirements: Dict, topology: Dict) -> Dict:
        """Optimize for network awareness"""
        recommendations = []
        
        # Check network performance
        perf = topology.get("performance_metrics", {})
        if perf.get("error_rate", 0) > 0.01:  # 1% error rate
            recommendations.append("🌐 High network error rate - consider reducing network usage")
        else:
            recommendations.append("🌐 Network performance looks good")
            
        return {
            "recommendations": recommendations,
            "network_quality_score": 1.0 - perf.get("error_rate", 0)
        }
        
    def _optimize_storage_access(self, job_requirements: Dict, topology: Dict) -> Dict:
        """Optimize storage access patterns"""
        recommendations = []
        
        # Check storage connectivity
        storage = topology.get("storage_connectivity", {})
        if len(storage.get("nfs_mounts", [])) > 3:
            recommendations.append("💾 Multiple NFS mounts - consider consolidating data access")
        else:
            recommendations.append("💾 Storage access pattern looks optimal")
            
        return {
            "recommendations": recommendations,
            "storage_efficiency_score": 0.9 if len(storage.get("nfs_mounts", [])) <= 3 else 0.6
        }

class HPCManagementIntegration:
    """Integration with HPC management systems"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.scheduler_type = self._detect_scheduler()
        self.management_systems = self._detect_management_systems()
        
    def _detect_scheduler(self) -> str:
        """Detect job scheduler type"""
        # Check for common schedulers
        if os.path.exists('/usr/bin/squeue') or os.path.exists('/usr/bin/sbatch'):
            return "slurm"
        elif os.path.exists('/usr/bin/qsub') or os.path.exists('/usr/bin/qstat'):
            return "pbs"
        elif os.path.exists('/usr/bin/bsub') or os.path.exists('/usr/bin/bjobs'):
            return "lsf"
        else:
            return "unknown"
            
    def _detect_management_systems(self) -> List[str]:
        """Detect available HPC management systems"""
        systems = []
        
        # Check for common HPC management tools
        management_tools = {
            "ganglia": ["gstat", "gmetric"],
            "nagios": ["nagios", "check_nagios"],
            "munin": ["munin-node"],
            "prometheus": ["prometheus"],
            "grafana": ["grafana-server"]
        }
        
        for system, tools in management_tools.items():
            for tool in tools:
                try:
                    result = subprocess.run(['which', tool], capture_output=True, timeout=5)
                    if result.returncode == 0:
                        systems.append(system)
                        break
                except:
                    continue
                    
        return systems
        
    def get_scheduler_info(self) -> Dict:
        """Get scheduler information"""
        info = {
            "scheduler_type": self.scheduler_type,
            "available_commands": [],
            "queue_info": {},
            "job_info": {}
        }
        
        if self.scheduler_type == "lsf":
            info["available_commands"] = ["bsub", "bjobs", "bqueues", "bhosts"]
            info["queue_info"] = self._get_lsf_queue_info()
            info["job_info"] = self._get_lsf_job_info()
        elif self.scheduler_type == "slurm":
            info["available_commands"] = ["sbatch", "squeue", "sinfo", "sacct"]
            info["queue_info"] = self._get_slurm_queue_info()
            info["job_info"] = self._get_slurm_job_info()
        elif self.scheduler_type == "pbs":
            info["available_commands"] = ["qsub", "qstat", "pbsnodes"]
            info["queue_info"] = self._get_pbs_queue_info()
            info["job_info"] = self._get_pbs_job_info()
            
        return info
        
    def _get_lsf_queue_info(self) -> Dict:
        """Get LSF queue information"""
        try:
            result = subprocess.run(['bqueues', '-w'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def _get_lsf_job_info(self) -> Dict:
        """Get LSF job information"""
        try:
            result = subprocess.run(['bjobs'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def _get_slurm_queue_info(self) -> Dict:
        """Get SLURM queue information"""
        try:
            result = subprocess.run(['sinfo'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def _get_slurm_job_info(self) -> Dict:
        """Get SLURM job information"""
        try:
            result = subprocess.run(['squeue'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def _get_pbs_queue_info(self) -> Dict:
        """Get PBS queue information"""
        try:
            result = subprocess.run(['qstat', '-q'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def _get_pbs_job_info(self) -> Dict:
        """Get PBS job information"""
        try:
            result = subprocess.run(['qstat'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return {"status": "available", "output": result.stdout}
        except:
            pass
        return {"status": "unavailable"}
        
    def integrate_with_management_systems(self) -> Dict:
        """Integrate with available HPC management systems"""
        integration_status = {}
        
        for system in self.management_systems:
            integration_status[system] = self._test_system_integration(system)
            
        return integration_status
        
    def _test_system_integration(self, system: str) -> Dict:
        """Test integration with specific management system"""
        try:
            if system == "ganglia":
                result = subprocess.run(['gstat'], capture_output=True, text=True, timeout=10)
                return {"status": "available", "test_result": result.returncode == 0}
            elif system == "nagios":
                result = subprocess.run(['check_nagios'], capture_output=True, text=True, timeout=10)
                return {"status": "available", "test_result": result.returncode == 0}
            else:
                return {"status": "unknown", "test_result": False}
        except:
            return {"status": "unavailable", "test_result": False}

def main():
    """Test the advanced features"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Advanced HPC Features')
    parser.add_argument('--test-storage-prediction', action='store_true', help='Test storage failure prediction')
    parser.add_argument('--test-resource-prediction', action='store_true', help='Test resource exhaustion prediction')
    parser.add_argument('--test-network-topology', action='store_true', help='Test network topology awareness')
    parser.add_argument('--test-hpc-integration', action='store_true', help='Test HPC management integration')
    
    args = parser.parse_args()
    
    # Load config
    config = {"monitoring": {"alert_thresholds": {"storage_usage": 0.95}}}
    
    if args.test_storage_prediction:
        print("🧪 Testing Storage Failure Prediction")
        predictor = StorageFailurePredictor(config)
        prediction = predictor.predict_storage_failure(0.996, 0.05)  # 99.6% usage, 5% growth
        print(json.dumps(prediction, indent=2))
        
    if args.test_resource_prediction:
        print("🧪 Testing Resource Exhaustion Prediction")
        predictor = ResourceExhaustionPredictor(config)
        current_resources = {
            "cpu": {"usage_percent": 55, "available": 57},
            "memory": {"usage_percent": 4.4, "available_gb": 1925},
            "storage": {"usage_percent": 99.6, "free_gb": 2061},
            "lsf": {"normal": {"pending": 50, "running": 0, "max_jobs": 100}}
        }
        prediction = predictor.predict_resource_exhaustion(current_resources)
        print(json.dumps(prediction, indent=2))
        
    if args.test_network_topology:
        print("🧪 Testing Network Topology Awareness")
        topology = NetworkTopologyAwareness(config)
        network_info = topology.discover_network_topology()
        print(json.dumps(network_info, indent=2))
        
    if args.test_hpc_integration:
        print("🧪 Testing HPC Management Integration")
        integration = HPCManagementIntegration(config)
        scheduler_info = integration.get_scheduler_info()
        management_status = integration.integrate_with_management_systems()
        print("Scheduler Info:", json.dumps(scheduler_info, indent=2))
        print("Management Systems:", json.dumps(management_status, indent=2))

if __name__ == '__main__':
    main()