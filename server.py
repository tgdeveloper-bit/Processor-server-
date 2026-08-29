# processor_server.py
# Complete Processor Server with Neon.tech PostgreSQL integration

import asyncio
import os
import uuid
import json
import logging
import random
import string
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv

# Import database
from database import init_db, db_manager, USE_DATABASE, get_db
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
PORT = int(os.getenv("PORT", "8004"))
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")
PASSWORD_CHANGE_SERVER_URL = os.getenv("PASSWORD_CHANGE_SERVER_URL", "http://localhost:8001")
SPAM_CHECK_SERVER_URL = os.getenv("SPAM_CHECK_SERVER_URL", "http://localhost:8002")
DEVICE_CHECK_SERVER_URL = os.getenv("DEVICE_CHECK_SERVER_URL", "http://localhost:8003")
MAIN_SERVER_CALLBACK_URL = os.getenv("MAIN_SERVER_CALLBACK_URL", "http://localhost:8000/internal/processor-callback")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))

# Task semaphore
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# In-memory fallback (if database is not configured)
active_tasks: Dict[str, Dict[str, Any]] = {}
background_tasks_refs = set()

# ==================== PYDANTIC MODELS ====================
class ProcessorRequest(BaseModel):
    """Main request from Main Server"""
    processor_id: str = Field(..., description="Unique processor ID")
    session_id: str = Field(..., description="Session ID from Main Server")
    phone_number: str = Field(..., description="Phone number with country code")
    session_string: str = Field(..., description="Pyrogram session string")
    two_fa_password: Optional[str] = Field(None, description="Current 2FA password")
    user_id: Optional[int] = Field(None, description="Telegram user ID")
    username: Optional[str] = Field(None, description="Telegram username")
    first_name: Optional[str] = Field(None, description="First name")
    last_name: Optional[str] = Field(None, description="Last name")
    endpoint_name: str = Field("TGLionV2_bot", description="Bot endpoint name")
    user_identifier: Optional[str] = Field(None, description="User identifier")
    user_identifier_type: Optional[str] = Field(None, description="Identifier type")
    bot_token: Optional[str] = Field(None, description="Bot token for notifications")
    admin_telegram_id: Optional[int] = Field(None, description="Admin Telegram ID")
    callback_url: str = Field(..., description="Callback URL for final result")
    
    # Optional customization
    generate_username: bool = Field(False, description="Generate random username")
    set_profile_name: Optional[str] = Field(None, description="Set profile name")
    set_profile_photo: Optional[str] = Field(None, description="Profile photo URL")
    clear_account: bool = Field(True, description="Clear account (leave chats, block bots)")
    leave_chats: bool = Field(True, description="Leave all groups/channels")
    block_bots: bool = Field(True, description="Block bots only")
    spam_check_required: bool = Field(True, description="Check spam status")
    
    @validator('phone_number')
    def validate_phone(cls, v):
        if not v.startswith('+'):
            raise ValueError('Phone number must start with +')
        return v
    
    @validator('session_string')
    def validate_session_string(cls, v):
        if len(v) < 20:
            raise ValueError('Invalid session string')
        return v

class HealthResponse(BaseModel):
    status: str
    active_tasks: int
    completed_tasks: int
    max_concurrent_tasks: int
    database_connected: bool
    timestamp: datetime

# ==================== UTILITY FUNCTIONS ====================
def generate_random_username(length: int = 12) -> str:
    """Generate random username"""
    letters = string.ascii_letters + string.digits
    username = ''.join(random.choice(letters) for _ in range(length))
    return username

def generate_random_name() -> str:
    """Generate random profile name"""
    first_names = [
        "Alex", "John", "Mike", "David", "Chris", "James", "Robert", "Daniel",
        "Sarah", "Emma", "Olivia", "Sophia", "Isabella", "Mia", "Charlotte", "Amelia"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee"
    ]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# ==================== HTTP CLIENT ====================
class ServiceClient:
    """HTTP client for calling other services"""
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=120.0)
        self.headers = {"X-Internal-Key": INTERNAL_API_KEY}
    
    async def call_password_change(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Password Change Server (Port 8001)"""
        try:
            response = await self.client.post(
                f"{PASSWORD_CHANGE_SERVER_URL}/process",
                json=data,
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Password change server error: {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
        except httpx.TimeoutException:
            logger.error("Password change server timeout")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            logger.error(f"Password change call failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def call_spam_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Spam Check Server (Port 8002)"""
        try:
            response = await self.client.post(
                f"{SPAM_CHECK_SERVER_URL}/process",
                json=data,
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Spam check server error: {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
        except httpx.TimeoutException:
            logger.error("Spam check server timeout")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            logger.error(f"Spam check call failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def call_device_check(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Call Device Check Server (Port 8003)"""
        try:
            response = await self.client.post(
                f"{DEVICE_CHECK_SERVER_URL}/process",
                json=data,
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 202:
                return response.json()
            else:
                logger.error(f"Device check server error: {response.status_code}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
        except httpx.TimeoutException:
            logger.error("Device check server timeout")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            logger.error(f"Device check call failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def send_callback(self, callback_url: str, data: Dict[str, Any]) -> bool:
        """Send callback to Main Server"""
        try:
            response = await self.client.post(
                callback_url,
                json=data,
                headers=self.headers
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Callback failed: {e}")
            return False
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

# ==================== PROCESSOR CLASS ====================
class Processor:
    """Main processor orchestrator"""
    
    def __init__(self):
        self.service_client = ServiceClient()
    
    async def process_session(self, request: ProcessorRequest) -> Dict[str, Any]:
        """Main processing pipeline"""
        session_id = request.session_id
        processor_id = request.processor_id
        start_time = time.time()
        
        logger.info(f"🔵 [Processor] Starting processing for session {session_id}")
        
        # Create task in database
        if USE_DATABASE:
            db_manager.create_task(
                session_id=session_id,
                processor_id=processor_id,
                phone_number=request.phone_number,
                user_id=request.user_id,
                username=request.username,
                endpoint_name=request.endpoint_name
            )
            db_manager.add_audit_log(session_id, processor_id, "process_started", "processing")
        
        # In-memory tracking
        active_tasks[session_id] = {
            "processor_id": processor_id,
            "status": "processing",
            "started_at": datetime.utcnow().isoformat(),
            "steps_completed": [],
            "steps_failed": []
        }
        
        steps_completed = []
        steps_failed = []
        final_data = {}
        
        try:
            # ========== STEP 1: PASSWORD CHANGE ==========
            logger.info(f"📝 [Step 1] Password change for {session_id}")
            
            new_password = ''.join(random.choices(string.ascii_letters + string.digits + "!@#$%^&*", k=16))
            
            password_change_data = {
                "session_id": session_id,
                "session_string": request.session_string,
                "phone_number": request.phone_number,
                "current_password": request.two_fa_password,
                "new_password": new_password,
                "admin_telegram_id": request.admin_telegram_id,
                "bot_token": request.bot_token or TELEGRAM_BOT_TOKEN,
                "endpoint_name": request.endpoint_name,
                "client_app_name": "TGLionV2",
                "client_app_version": "2.0.0",
                "callback_url": MAIN_SERVER_CALLBACK_URL
            }
            
            password_result = await self.service_client.call_password_change(password_change_data)
            
            if password_result.get("success"):
                steps_completed.append("password_change")
                final_data["password_change"] = {
                    "success": True,
                    "new_password": password_result.get("new_password", new_password),
                    "new_session_string": password_result.get("new_session_string")
                }
                logger.info(f"✅ [Step 1] Password changed successfully for {session_id}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "password_changed", "success")
                
                if password_result.get("new_session_string"):
                    request.session_string = password_result["new_session_string"]
            else:
                steps_failed.append("password_change")
                error_msg = password_result.get("message") or password_result.get("error") or "Password change failed"
                logger.error(f"❌ [Step 1] Password change failed for {session_id}: {error_msg}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "password_changed", "failed", error_message=error_msg)
                
                return await self._finalize_failure(
                    request, steps_completed, steps_failed,
                    f"Step 1 (Password Change) failed: {error_msg}",
                    start_time
                )
            
            # ========== STEP 2: SPAM CHECK & ACCOUNT CLEANUP ==========
            logger.info(f"🔍 [Step 2] Spam check and cleanup for {session_id}")
            
            new_username = None
            if request.generate_username:
                new_username = generate_random_username()
                logger.info(f"🎲 [Step 2] Generated username: {new_username}")
            
            profile_name = request.set_profile_name
            if not profile_name and request.generate_username:
                profile_name = generate_random_name()
                logger.info(f"🎲 [Step 2] Generated profile name: {profile_name}")
            
            spam_check_data = {
                "session_id": session_id,
                "session_string": request.session_string,
                "phone_number": request.phone_number,
                "password": password_result.get("new_password", new_password) if password_result.get("success") else request.two_fa_password,
                "clear_account": request.clear_account,
                "set_name": profile_name,
                "set_username": new_username,
                "set_profile_photo": request.set_profile_photo,
                "leave_chats": request.leave_chats,
                "block_bots": request.block_bots,
                "spam_check_required": request.spam_check_required,
                "endpoint_name": request.endpoint_name,
                "callback_url": MAIN_SERVER_CALLBACK_URL
            }
            
            spam_result = await self.service_client.call_spam_check(spam_check_data)
            
            if spam_result.get("success") or spam_result.get("cleared_status") is not None:
                steps_completed.append("spam_check")
                final_data["spam_check"] = {
                    "spam_status": spam_result.get("spam_status", "unknown"),
                    "spam_details": spam_result.get("spam_details", ""),
                    "cleared_status": spam_result.get("cleared_status", False),
                    "profile_updated": spam_result.get("profile_updated", False),
                    "message": spam_result.get("message", "Spam check completed")
                }
                logger.info(f"✅ [Step 2] Spam check completed for {session_id}: {spam_result.get('spam_status', 'unknown')}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "spam_checked", "success", 
                                           details={"spam_status": spam_result.get("spam_status")})
                
                if spam_result.get("spam_status") == "banned":
                    logger.warning(f"⚠️ [Step 2] Account is banned for {session_id}")
                    return await self._finalize_failure(
                        request, steps_completed, steps_failed,
                        "Account is banned",
                        start_time
                    )
            else:
                steps_failed.append("spam_check")
                error_msg = spam_result.get("error") or "Spam check failed"
                logger.error(f"❌ [Step 2] Spam check failed for {session_id}: {error_msg}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "spam_checked", "failed", error_message=error_msg)
            
            # ========== STEP 3: DEVICE CHECK & TERMINATION ==========
            logger.info(f"📱 [Step 3] Device check for {session_id}")
            
            device_check_data = {
                "session_id": session_id,
                "session_string": request.session_string,
                "phone_number": request.phone_number,
                "endpoint_name": request.endpoint_name,
                "callback_url": MAIN_SERVER_CALLBACK_URL,
                "retry_callback_url": f"{MAIN_SERVER_CALLBACK_URL}/retry"
            }
            
            device_result = await self.service_client.call_device_check(device_check_data)
            
            if device_result.get("success"):
                steps_completed.append("device_check")
                final_data["device_check"] = {
                    "success": True,
                    "termination_status": device_result.get("device_termination_status", "completed"),
                    "other_devices_terminated": device_result.get("other_devices_terminated", 0),
                    "total_devices": device_result.get("total_devices", 0)
                }
                logger.info(f"✅ [Step 3] Device check completed for {session_id}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "device_checked", "success")
            elif device_result.get("device_termination_status") == "waiting_24h" or \
                 device_result.get("retry_after_hours"):
                steps_completed.append("device_check_scheduled")
                final_data["device_check"] = {
                    "success": False,
                    "status": "waiting_retry",
                    "retry_after_hours": device_result.get("retry_after_hours", 24),
                    "message": "Device termination scheduled for retry"
                }
                logger.info(f"⏰ [Step 3] Device check needs 24h wait for {session_id}")
                
                return await self._finalize_retry(
                    request, steps_completed, steps_failed,
                    final_data, "Device termination needs 24h wait",
                    device_result.get("retry_after_hours", 24),
                    start_time
                )
            else:
                steps_failed.append("device_check")
                error_msg = device_result.get("message") or device_result.get("error") or "Device check failed"
                logger.error(f"❌ [Step 3] Device check failed for {session_id}: {error_msg}")
                
                if USE_DATABASE:
                    db_manager.add_audit_log(session_id, processor_id, "device_checked", "failed", error_message=error_msg)
                
                return await self._finalize_failure(
                    request, steps_completed, steps_failed,
                    f"Step 3 (Device Check) failed: {error_msg}",
                    start_time
                )
            
            # ========== ALL STEPS COMPLETED ==========
            logger.info(f"🎉 [Processor] All steps completed for {session_id}")
            
            total_time = int((time.time() - start_time) * 1000)
            
            result = {
                "processor_id": processor_id,
                "session_id": session_id,
                "status": "completed",
                "steps_completed": steps_completed,
                "steps_failed": steps_failed,
                "final_data": final_data,
                "total_processing_time_ms": total_time,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Update database
            if USE_DATABASE:
                db_manager.update_task(
                    session_id=session_id,
                    status="completed",
                    steps_completed=steps_completed,
                    steps_failed=steps_failed,
                    final_data=final_data,
                    total_time_ms=total_time
                )
                db_manager.add_audit_log(session_id, processor_id, "process_completed", "success")
            
            # Send callback
            asyncio.create_task(self.service_client.send_callback(request.callback_url, result))
            
            # Clean up
            active_tasks.pop(session_id, None)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ [Processor] Unexpected error for {session_id}: {e}", exc_info=True)
            return await self._finalize_failure(
                request, steps_completed, steps_failed,
                f"Unexpected error: {str(e)}",
                start_time
            )
    
    async def _finalize_failure(self, request, steps_completed, steps_failed, error_message, start_time):
        """Finalize failure result"""
        session_id = request.session_id
        total_time = int((time.time() - start_time) * 1000)
        
        result = {
            "processor_id": request.processor_id,
            "session_id": session_id,
            "status": "failed",
            "steps_completed": steps_completed,
            "steps_failed": steps_failed,
            "error_message": error_message,
            "total_processing_time_ms": total_time,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if USE_DATABASE:
            db_manager.update_task(
                session_id=session_id,
                status="failed",
                steps_completed=steps_completed,
                steps_failed=steps_failed,
                error_message=error_message,
                total_time_ms=total_time
            )
            db_manager.add_audit_log(session_id, request.processor_id, "process_failed", "failed", error_message=error_message)
        
        asyncio.create_task(self.service_client.send_callback(request.callback_url, result))
        active_tasks.pop(session_id, None)
        
        return result
    
    async def _finalize_retry(self, request, steps_completed, steps_failed, final_data, message, retry_after_hours, start_time):
        """Finalize retry result"""
        session_id = request.session_id
        total_time = int((time.time() - start_time) * 1000)
        
        result = {
            "processor_id": request.processor_id,
            "session_id": session_id,
            "status": "waiting_retry",
            "steps_completed": steps_completed,
            "steps_failed": steps_failed,
            "final_data": final_data,
            "error_message": message,
            "retry_after_hours": retry_after_hours,
            "total_processing_time_ms": total_time,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if USE_DATABASE:
            db_manager.update_task(
                session_id=session_id,
                status="waiting_retry",
                steps_completed=steps_completed,
                steps_failed=steps_failed,
                final_data=final_data,
                error_message=message,
                retry_after_hours=retry_after_hours,
                total_time_ms=total_time
            )
        
        asyncio.create_task(self.service_client.send_callback(request.callback_url, result))
        active_tasks.pop(session_id, None)
        
        return result
    
    async def close(self):
        """Close service client"""
        await self.service_client.close()

# ==================== FASTAPI APP ====================
processor = Processor()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager"""
    logger.info("🚀 Starting Processor Server...")
    logger.info(f"📡 Password Change Server: {PASSWORD_CHANGE_SERVER_URL}")
    logger.info(f"📡 Spam Check Server: {SPAM_CHECK_SERVER_URL}")
    logger.info(f"📡 Device Check Server: {DEVICE_CHECK_SERVER_URL}")
    logger.info(f"📡 Main Server Callback: {MAIN_SERVER_CALLBACK_URL}")
    logger.info(f"⚡ Max Concurrent Tasks: {MAX_CONCURRENT_TASKS}")
    logger.info(f"🔌 Running on port: {PORT}")
    logger.info(f"💾 Database: {'Connected' if USE_DATABASE else 'Not connected (using memory)'}")
    
    # Initialize database
    if USE_DATABASE:
        init_db()
    
    yield
    
    logger.info("🔧 Shutting down Processor Server...")
    await processor.close()
    for task in background_tasks_refs:
        task.cancel()
    background_tasks_refs.clear()

app = FastAPI(
    title="Processor Server",
    description="Orchestrates Password Change, Spam Check, and Device Check",
    version="3.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== AUTHENTICATION ====================
async def verify_internal_key(x_internal_key: str = Header(..., alias="X-Internal-Key")):
    """Verify internal API key"""
    if not INTERNAL_API_KEY:
        logger.warning("INTERNAL_API_KEY not set, authentication disabled")
        return True
    
    if x_internal_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")
    
    return True

# ==================== ENDPOINTS ====================
@app.post("/process")
async def process_request(
    request: ProcessorRequest,
    authenticated: bool = Depends(verify_internal_key)
):
    """Process session through all steps"""
    session_id = request.session_id
    logger.info(f"📥 Received process request for session {session_id}")
    
    # Check if already processing
    if session_id in active_tasks:
        return {
            "success": False,
            "message": "Session is already being processed",
            "session_id": session_id,
            "processor_id": request.processor_id
        }
    
    # Check database for existing task
    if USE_DATABASE:
        existing_task = db_manager.get_task(session_id)
        if existing_task and existing_task.get("status") == "completed":
            return {
                "success": True,
                "message": "Returning cached result",
                "session_id": session_id,
                "result": existing_task
            }
    
    # Create background task
    async def background_process():
        try:
            async with task_semaphore:
                result = await processor.process_session(request)
                logger.info(f"✅ Background processing completed for {session_id}")
        except Exception as e:
            logger.error(f"❌ Background processing failed for {session_id}: {e}", exc_info=True)
        finally:
            background_tasks_refs.discard(asyncio.current_task())
    
    task = asyncio.create_task(background_process())
    background_tasks_refs.add(task)
    
    return {
        "success": True,
        "message": "Processing started",
        "processor_id": request.processor_id,
        "session_id": session_id,
        "task_status": "processing"
    }

@app.get("/task/{session_id}")
async def get_task_status(
    session_id: str,
    authenticated: bool = Depends(verify_internal_key)
):
    """Get task status by session ID"""
    
    # Check active tasks
    if session_id in active_tasks:
        return {
            "session_id": session_id,
            "task_status": "processing",
            "details": active_tasks[session_id]
        }
    
    # Check database
    if USE_DATABASE:
        task = db_manager.get_task(session_id)
        if task:
            return {
                "session_id": session_id,
                "task_status": task["status"],
                "result": task
            }
    
    return {
        "session_id": session_id,
        "task_status": "not_found",
        "error": "No task found for this session ID"
    }

@app.get("/tasks/recent")
async def get_recent_tasks(
    limit: int = 50,
    authenticated: bool = Depends(verify_internal_key)
):
    """Get recent tasks from database"""
    if USE_DATABASE:
        tasks = db_manager.get_recent_tasks(limit)
        return {
            "count": len(tasks),
            "tasks": tasks
        }
    else:
        return {
            "count": 0,
            "tasks": [],
            "message": "Database not connected"
        }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    active_count = len(active_tasks)
    completed_count = db_manager.get_completed_tasks_count() if USE_DATABASE else 0
    
    return HealthResponse(
        status="healthy",
        active_tasks=active_count,
        completed_tasks=completed_count,
        max_concurrent_tasks=MAX_CONCURRENT_TASKS,
        database_connected=USE_DATABASE,
        timestamp=datetime.utcnow()
    )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Processor Server",
        "version": "3.0.0",
        "description": "Orchestrates Password Change, Spam Check, and Device Check",
        "status": "running",
        "database": "connected" if USE_DATABASE else "not connected",
        "endpoints": {
            "process": "/process",
            "task_status": "/task/{session_id}",
            "recent_tasks": "/tasks/recent",
            "health": "/health"
        }
    }

# ==================== ERROR HANDLERS ====================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Global exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "detail": str(exc) if os.getenv("DEBUG", "false").lower() == "true" else None
        }
    )

# ==================== MAIN ENTRY ====================
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        reload=False,
        log_level="info",
        workers=1
    )