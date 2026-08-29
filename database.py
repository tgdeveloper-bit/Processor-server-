# database.py
# Database configuration for Neon.tech PostgreSQL

import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    create_engine, Column, String, Integer, DateTime, 
    JSON, Text, Boolean, Float, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# ==================== DATABASE CONFIGURATION ====================
# Neon.tech provides a PostgreSQL connection string
# Format: postgresql://user:password@host/database?sslmode=require
DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL:
    logger.warning("DATABASE_URL not set. Using in-memory storage fallback.")
    USE_DATABASE = False
else:
    # Ensure SSL mode for Neon.tech
    if "sslmode" not in DATABASE_URL:
        if "?" in DATABASE_URL:
            DATABASE_URL += "&sslmode=require"
        else:
            DATABASE_URL += "?sslmode=require"
    
    USE_DATABASE = True

# Create engine with connection pooling
if USE_DATABASE:
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,  # Recycle connections every 30 minutes
        echo=False
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
else:
    engine = None
    SessionLocal = None

Base = declarative_base()

# ==================== DATABASE MODELS ====================
class TaskRecord(Base):
    """Task record for storing processing results"""
    __tablename__ = "processor_tasks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), unique=True, index=True, nullable=False)
    processor_id = Column(String(255), index=True, nullable=False)
    phone_number = Column(String(50), index=True)
    status = Column(String(50), index=True, nullable=False)  # processing, completed, failed, waiting_retry
    
    # Processing details
    steps_completed = Column(JSON, default=list)
    steps_failed = Column(JSON, default=list)
    final_data = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_after_hours = Column(Integer, nullable=True)
    
    # Timing
    total_processing_time_ms = Column(Integer)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Additional metadata
    user_id = Column(Integer, nullable=True)
    username = Column(String(255), nullable=True)
    endpoint_name = Column(String(255), default="TGLionV2_bot")
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_session_status', 'session_id', 'status'),
        Index('idx_created_at', 'created_at'),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "processor_id": self.processor_id,
            "phone_number": self.phone_number,
            "status": self.status,
            "steps_completed": self.steps_completed or [],
            "steps_failed": self.steps_failed or [],
            "final_data": self.final_data,
            "error_message": self.error_message,
            "retry_after_hours": self.retry_after_hours,
            "total_processing_time_ms": self.total_processing_time_ms,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "user_id": self.user_id,
            "username": self.username,
            "endpoint_name": self.endpoint_name
        }

class AuditLog(Base):
    """Audit log for tracking all operations"""
    __tablename__ = "audit_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), index=True)
    processor_id = Column(String(255), index=True)
    action = Column(String(100), index=True)  # process_started, password_changed, spam_checked, etc.
    status = Column(String(50))  # success, failed, pending
    details = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "processor_id": self.processor_id,
            "action": self.action,
            "status": self.status,
            "details": self.details,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

# ==================== DATABASE FUNCTIONS ====================
def init_db():
    """Initialize database tables"""
    if USE_DATABASE and engine:
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("✅ Database tables created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create database tables: {e}")

def get_db():
    """Dependency to get database session"""
    if not USE_DATABASE:
        return None
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DatabaseManager:
    """Database manager for task operations"""
    
    @staticmethod
    def create_task(session_id: str, processor_id: str, phone_number: str, 
                   user_id: Optional[int] = None, username: Optional[str] = None,
                   endpoint_name: str = "TGLionV2_bot") -> Optional[TaskRecord]:
        """Create new task record"""
        if not USE_DATABASE:
            return None
        
        db = SessionLocal()
        try:
            task = TaskRecord(
                session_id=session_id,
                processor_id=processor_id,
                phone_number=phone_number,
                status="processing",
                user_id=user_id,
                username=username,
                endpoint_name=endpoint_name,
                started_at=datetime.utcnow()
            )
            db.add(task)
            db.commit()
            db.refresh(task)
            logger.info(f"✅ Task created in database: {session_id}")
            return task
        except Exception as e:
            logger.error(f"❌ Failed to create task: {e}")
            db.rollback()
            return None
        finally:
            db.close()
    
    @staticmethod
    def update_task(session_id: str, status: str, steps_completed: List[str] = None,
                   steps_failed: List[str] = None, final_data: Dict = None,
                   error_message: str = None, retry_after_hours: int = None,
                   total_time_ms: int = None) -> bool:
        """Update task record"""
        if not USE_DATABASE:
            return False
        
        db = SessionLocal()
        try:
            task = db.query(TaskRecord).filter(TaskRecord.session_id == session_id).first()
            if not task:
                logger.warning(f"Task not found: {session_id}")
                return False
            
            task.status = status
            if steps_completed is not None:
                task.steps_completed = steps_completed
            if steps_failed is not None:
                task.steps_failed = steps_failed
            if final_data is not None:
                task.final_data = final_data
            if error_message is not None:
                task.error_message = error_message
            if retry_after_hours is not None:
                task.retry_after_hours = retry_after_hours
            if total_time_ms is not None:
                task.total_processing_time_ms = total_time_ms
            
            if status in ["completed", "failed", "waiting_retry"]:
                task.completed_at = datetime.utcnow()
            
            task.updated_at = datetime.utcnow()
            db.commit()
            logger.info(f"✅ Task updated in database: {session_id} -> {status}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update task: {e}")
            db.rollback()
            return False
        finally:
            db.close()
    
    @staticmethod
    def get_task(session_id: str) -> Optional[Dict[str, Any]]:
        """Get task by session ID"""
        if not USE_DATABASE:
            return None
        
        db = SessionLocal()
        try:
            task = db.query(TaskRecord).filter(TaskRecord.session_id == session_id).first()
            return task.to_dict() if task else None
        except Exception as e:
            logger.error(f"❌ Failed to get task: {e}")
            return None
        finally:
            db.close()
    
    @staticmethod
    def get_recent_tasks(limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent tasks"""
        if not USE_DATABASE:
            return []
        
        db = SessionLocal()
        try:
            tasks = db.query(TaskRecord).order_by(TaskRecord.created_at.desc()).limit(limit).all()
            return [task.to_dict() for task in tasks]
        except Exception as e:
            logger.error(f"❌ Failed to get recent tasks: {e}")
            return []
        finally:
            db.close()
    
    @staticmethod
    def add_audit_log(session_id: str, processor_id: str, action: str, 
                     status: str, details: Dict = None, error_message: str = None) -> bool:
        """Add audit log entry"""
        if not USE_DATABASE:
            return False
        
        db = SessionLocal()
        try:
            log = AuditLog(
                session_id=session_id,
                processor_id=processor_id,
                action=action,
                status=status,
                details=details,
                error_message=error_message
            )
            db.add(log)
            db.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Failed to add audit log: {e}")
            db.rollback()
            return False
        finally:
            db.close()
    
    @staticmethod
    def get_active_tasks_count() -> int:
        """Get count of active tasks"""
        if not USE_DATABASE:
            return 0
        
        db = SessionLocal()
        try:
            return db.query(TaskRecord).filter(TaskRecord.status == "processing").count()
        except Exception as e:
            logger.error(f"❌ Failed to get active tasks count: {e}")
            return 0
        finally:
            db.close()
    
    @staticmethod
    def get_completed_tasks_count() -> int:
        """Get count of completed tasks"""
        if not USE_DATABASE:
            return 0
        
        db = SessionLocal()
        try:
            return db.query(TaskRecord).filter(TaskRecord.status.in_(["completed", "failed", "waiting_retry"])).count()
        except Exception as e:
            logger.error(f"❌ Failed to get completed tasks count: {e}")
            return 0
        finally:
            db.close()

# Initialize database manager
db_manager = DatabaseManager()