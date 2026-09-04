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
from pyrogram.types import User
from pyrogram.raw.functions.messages import DeleteHistory
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
PROCESSOR_SERVER_URL = os.getenv('PROCESSOR_SERVER_URL', 'http://localhost:8004')
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
    {"hours": 24, "retry_count": 4}
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
            command_timeout=60,
            statement_cache_size=0  # Disable statement cache
        )
        await self.create_tables()
    
        logger.info("Database pool initialized with statement cache disabled")
    
    async def create_tables(self):
        async with self.pool.acquire() as conn:
            # Main processing jobs table with step status columns
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
                    step1_status VARCHAR(50) DEFAULT 'pending',
                    step2_status VARCHAR(50) DEFAULT 'pending',
                    step3_status VARCHAR(50) DEFAULT 'pending',
                    step1_retry_count INTEGER DEFAULT 0,
                    step2_retry_count INTEGER DEFAULT 0,
                    step3_retry_count INTEGER DEFAULT 0,
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
            
            # Proxies table - এই অংশটা পরিবর্তন করুন
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS proxies (
                    id SERIAL PRIMARY KEY,
                    country_code VARCHAR(10),      -- country এর বদলে country_code
                    scheme VARCHAR(10) DEFAULT 'socks5',  -- নতুন
                    hostname VARCHAR(255) NOT NULL, -- host এর বদলে hostname
                    port INTEGER NOT NULL,
                    username VARCHAR(100),
                    password VARCHAR(100),
                    ping_ms INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    max_sessions INTEGER DEFAULT 10,  -- নতুন
                    current_sessions INTEGER DEFAULT 0, -- নতুন
                    last_checked TIMESTAMP,           -- নতুন
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Index ও পরিবর্তন করুন
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_proxies_country 
                ON proxies(country_code, is_active)  -- country এর বদলে country_code
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
            async with conn.transaction():
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
    
    async def update_step_status(self, job_id: int, step_number: int, status: str):
        """Update a specific step's status"""
        step_column = f"step{step_number}_status"
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                UPDATE processing_jobs SET {step_column} = $1, updated_at = NOW() WHERE id = $2
            ''', status, job_id)

    async def increment_step_retry_count(self, job_id: int, step_number: int):
        """Increment retry count for a specific step"""
        retry_column = f"step{step_number}_retry_count"
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                UPDATE processing_jobs SET {retry_column} = {retry_column} + 1, updated_at = NOW() WHERE id = $1
            ''', job_id)

    async def get_step_status(self, job_id: int, step_number: int) -> str:
        """Get status of a specific step"""
        step_column = f"step{step_number}_status"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT {step_column} FROM processing_jobs WHERE id = $1", job_id)
            return row[step_column] if row else 'pending'

    async def update_job_payload(self, job_id: int, payload: Dict[str, Any]):
        """Update job payload"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE processing_jobs SET payload = $1, updated_at = NOW() WHERE id = $2
            ''', json.dumps(payload), job_id)
    
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
    
    async def get_proxy_for_country(self, country_code: str) -> Optional[Dict[str, Any]]:
        """Fetch an active proxy for the given country with lowest ping"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT hostname, port, username, password, ping_ms, scheme
                FROM proxies
                WHERE country_code = $1 AND is_active = TRUE 
                AND current_sessions < max_sessions
                ORDER BY ping_ms ASC NULLS LAST
                LIMIT 1
            ''', country_code)
            
            if row:
                proxy_dict = {
                    "scheme": row['scheme'] or 'socks5',
                    "hostname": row['hostname'],
                    "port": row['port'],
                    "username": row['username'],
                    "password": row['password'],
                    "ping_ms": row['ping_ms']
                }
                
                # Increment current_sessions
                await conn.execute('''
                    UPDATE proxies SET current_sessions = current_sessions + 1 
                    WHERE hostname = $1 AND port = $2
                ''', row['hostname'], row['port'])
                
                return proxy_dict
            return None
        
    async def release_proxy(self, hostname: str, port: int):
        """Release proxy session after use"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE proxies SET current_sessions = GREATEST(current_sessions - 1, 0)
                WHERE hostname = $1 AND port = $2
            ''', hostname, port)

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
async def step1_password_change(
        session_string: str,
        phone_number: str,
        current_password: Optional[str],
        new_password: str,
        admin_telegram_id: Optional[int],
        bot_token: Optional[str],
        proxy: Optional[Dict] = None
    ) -> Dict[str, Any]:
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step1_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            proxy=proxy,
            in_memory=True,
            no_updates=True
        )
        await client.start()
        
        # Remove existing 2FA if password provided
        if current_password:
            try:
                await client.remove_cloud_password(password=current_password)
                logger.info(f"✅ Old 2FA removed for {phone_number}")
            except PasswordHashInvalid:
                return {
                    "success": False,
                    "error": "PasswordHashInvalid - Wrong current password",
                    "processing_time_ms": int((time.time() - start_time) * 1000)
                }
        
        # Set new 2FA password (no recovery email)
        await client.enable_cloud_password(password=new_password)
        logger.info(f"✅ New 2FA set for {phone_number} (no recovery email)")
        
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
                                    f"⚠️ Save this password securely!\n"
                                    f"📧 Recovery Email: None (removed)",
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
            "recovery_email": None,  # Explicitly no recovery email
            "notification_sent": notification_sent,
            "processing_time_ms": processing_time_ms
        }
        
    except FloodWait as e:
        raise FloodWaitException(f"FloodWait: {e.value}s", e.value)
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
async def step2_spam_check(
    session_string: str, 
    phone_number: str,
    password: Optional[str], 
    set_name: Optional[str],
    clear_account: bool = True, 
    leave_chats: bool = True,
    set_profile_photo: Optional[str] = None,
    set_bio: Optional[str] = None,
    block_bots: bool = True, 
    proxy: Optional[Dict] = None, 
    spam_check_required: bool = True,
    set_username: Optional[str] = None
) -> Dict[str, Any]:
    """Step 2: Spam check and account cleanup"""
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step2_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            proxy=proxy,
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
            except FloodWait as e:
                raise FloodWaitException(f"FloodWait in spam check: {e.value}s", e.value)
            except Exception as e:
                spam_status = "unknown"
                spam_details = f"Error checking spam: {str(e)}"
                logger.warning(f"Spam check error: {e}")
        
        # Cleanup
        cleared_status = False
        total_processed = 0
        left_chats = 0
        blocked_bots = 0
        
        # Cleanup section - পরিবর্তিত version
        if clear_account:
            try:
                dialogs = []
                async for dialog in client.get_dialogs():
                    dialogs.append(dialog)

                logger.info(f"Total dialogs: {len(dialogs)}")
                bot_count = 0

                for idx, dialog in enumerate(dialogs):
                    chat = dialog.chat
                    chat_id = chat.id
                    chat_type = chat.type
                    chat_title = getattr(chat, 'title', '') or getattr(chat, 'first_name', '') or str(chat_id)

                    # Bot check - ChatType.BOT ব্যবহার করুন
                    is_bot = (chat_type == ChatType.BOT)
                    
                    if not is_bot and hasattr(chat, 'is_bot'):
                        is_bot = chat.is_bot

                    if is_bot:
                        bot_count += 1
                        logger.info(f"Found bot: {chat_title} (ID: {chat_id})")

                    try:
                        operation_performed = False

                        # Leave groups/channels
                        if leave_chats and chat_type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
                            await client.leave_chat(chat_id, delete=True)
                            left_chats += 1
                            operation_performed = True

                        # Block bots - সঠিক flow (delete আগে, block পরে)
                        if block_bots and is_bot:
                            try:
                                # Step 1: Chat history delete করুন
                                try:
                                    await client.invoke(
                                        DeleteHistory(
                                            peer=await client.resolve_peer(chat_id),
                                            max_id=0,
                                            just_clear=True,
                                            revoke=True
                                        )
                                    )
                                    logger.info(f"Deleted history for bot: {chat_title}")
                                except Exception as delete_error:
                                    logger.warning(f"Failed to delete history for {chat_title}: {delete_error}")
                                
                                # Step 2: Bot block করুন
                                await client.block_user(chat_id)
                                blocked_bots += 1
                                operation_performed = True
                                logger.info(f"✅ Blocked bot: {chat_title}")
                        
                            except FloodWait as e:
                                wait = min(e.value, 60)
                                await asyncio.sleep(wait)
                            except Exception as block_error:
                                logger.warning(f"Failed to block bot {chat_title}: {block_error}")

                        if operation_performed:
                            total_processed += 1

                            # Rate limiting - bot block এর জন্য slow করুন
                            if total_processed <= 10:
                                await asyncio.sleep(1)  # Bot block এর জন্য 1 second
                            elif total_processed <= 30:
                                await asyncio.sleep(2)  # 30 এর পরে 2 seconds
                            elif total_processed <= 50:
                                await asyncio.sleep(3)  # 50 এর পরে 3 seconds
                            else:
                                await asyncio.sleep(5)  # 50+ হলে 5 seconds

                    except FloodWait as e:
                        wait = min(e.value, 60)
                        logger.warning(f"FloodWait: waiting {wait}s")
                        await asyncio.sleep(wait)
                    except Exception as e:
                        logger.warning(f"Error processing {chat_title}: {e}")
                        continue

                logger.info(f"Cleanup summary - Total dialogs: {len(dialogs)}, Bots found: {bot_count}, Bots blocked: {blocked_bots}, Chats left: {left_chats}")
                cleared_status = total_processed > 0
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")
        
        # Profile update
        profile_updated = False
        updated_username = set_username

        # Update name & bio
        if set_name or set_bio:
            try:
                # Get current profile info
                me = await client.get_me()
                update_kwargs = {
                    "first_name": me.first_name or "User",
                    "last_name": me.last_name or ""
                }
                
                # Set name
                if set_name:
                    names = set_name.split(" ", 1)
                    update_kwargs["first_name"] = names[0]
                    if len(names) > 1:
                        update_kwargs["last_name"] = names[1]
        
                # Set bio
                if set_bio:
                    update_kwargs["bio"] = set_bio
        
                await client.update_profile(**update_kwargs)
                profile_updated = True
                logger.info(f"Profile updated: name={update_kwargs.get('first_name')}, bio={set_bio}")
            except Exception as e:
                logger.warning(f"Failed to update name/bio: {e}")

        # Update username
        # Update username - শুধু set করবে, change করবে না
        if set_username:
            try:
                # আগে check করুন current username কি
                me = await client.get_me()
                current_username = me.username
                
                # যদি current username থাকে → Skip
                if current_username:
                    updated_username = current_username
                    logger.info(f"Username already exists: @{current_username}, skipping set")
                else:
                    # Username নেই → Set করুন
                    try:
                        await client.update_username(set_username)
                        updated_username = set_username
                        profile_updated = True
                        logger.info(f"Username set: @{set_username}")
                    except UsernameOccupied:
                        # Username নেওয়া আছে, random suffix যোগ করুন
                        random_username = f"{set_username}_{random.randint(100, 999)}"
                        await client.update_username(random_username)
                        updated_username = random_username
                        profile_updated = True
                        logger.info(f"Username occupied, set with suffix: @{random_username}")
                    except Exception as e:
                        logger.warning(f"Failed to set username: {e}")
            except Exception as e:
                logger.warning(f"Error checking username: {e}")

        # Update profile photo
        if set_profile_photo:
            try:
                logger.info(f"Attempting to download photo from: {set_profile_photo}")
                        
                async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http:
                    response = await http.get(set_profile_photo)
                    logger.info(f"Download response: {response.status_code}")
                    logger.info(f"Content-Type: {response.headers.get('content-type')}")
                    logger.info(f"Content-Length: {len(response.content)} bytes")
            
                    if response.status_code == 200 and len(response.content) > 0:
                        photo_path = f"/tmp/photo_{uuid.uuid4().hex[:8]}.jpg"
                
                        with open(photo_path, 'wb') as f:
                            f.write(response.content)
                
                        logger.info(f"Photo saved to: {photo_path}")
                
                        try:
                            await client.set_profile_photo(photo=photo_path)
                            profile_updated = True
                            logger.info(f"✅ Profile photo set successfully!")
                        except Exception as photo_error:
                            logger.error(f"Failed to set profile photo: {photo_error}")
                
                        os.remove(photo_path)
                    else:
                        logger.warning(f"Failed to download photo: HTTP {response.status_code}, Size: {len(response.content)}")
            except Exception as e:
                logger.error(f"Photo update error: {e}")
        
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
            "updated_username": updated_username,
            "processing_time_ms": processing_time_ms
        }
        
    except FloodWait as e:
        raise FloodWaitException(f"FloodWait in step 2: {e.value}s", e.value)
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
async def step3_device_check(
        session_string: str,
        phone_number: str,
        proxy: Optional[Dict] = None
    ) -> Dict[str, Any]:
    client = None
    start_time = time.time()
    
    try:
        client = Client(
            name=f"step3_{uuid.uuid4().hex[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            proxy=proxy,
            in_memory=True,
            no_updates=True
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
        
    except FloodWait as e:
        raise FloodWaitException(f"FloodWait in step 3: {e.value}s", e.value)
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
    """Process a complete job through all 3 steps with step-wise retry"""
    job_id = job['id']
    session_id = str(job['session_id'])
    payload = job.get('payload', {})
    if isinstance(payload, str):
        payload = json.loads(payload)
    use_proxy = payload.get('use_proxy', False)
    country_code = payload.get('country_code')
    proxy = None
    if use_proxy and country_code:
        proxy = await db.get_proxy_for_country(country_code)
        if not proxy:
            raise Exception(f"No active proxy available for country {country_code}")
        logger.info(f"Using proxy for {country_code}: {proxy['hostname']}:{proxy['port']}")
    
    logger.info(f"🔨 Processing job {job_id} for session {session_id}")
    
    # Check which steps are already completed
    step1_done = job.get('step1_status') == 'completed'
    step2_done = job.get('step2_status') == 'completed'
    step3_done = job.get('step3_status') == 'completed'
    
    # Get new password from payload or generate new one
    new_password = payload.get('new_password') or generate_secure_password()
    random_name = generate_random_name()
    
    current_session_string = payload.get('session_string')
    step_results = {}
    
    # ==================== STEP 1: Password Change ====================
    if not step1_done:
        logger.info(f"Step 1: Password change for {session_id}")
        await db.update_job_step(job_id, 1)
        await db.update_step_status(job_id, 1, 'processing')
        
        step1_result = await step1_password_change(
            session_string=current_session_string,
            phone_number=payload['phone_number'],
            current_password=payload.get('current_password'),
            new_password=new_password,
            proxy=proxy,
            admin_telegram_id=payload.get('admin_telegram_id'),
            bot_token=payload.get('bot_token')
        )
        
        if step1_result.get('success'):
            await db.update_step_status(job_id, 1, 'completed')
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
            
            # Update session string and payload
            if step1_result.get('new_session_string'):
                current_session_string = step1_result['new_session_string']
                payload['session_string'] = step1_result['new_session_string']
                payload['new_password'] = new_password
                await db.update_job_payload(job_id, payload)
            
            step_results['step1'] = step1_result
            logger.info(f"Step 1 completed for {session_id}")
        else:
            await db.update_step_status(job_id, 1, 'failed')
            await db.increment_step_retry_count(job_id, 1)
            await db.save_step_result(
                job_id, session_id, 1, "password_change", "failed",
                error_message=step1_result.get('error'),
                processing_time_ms=step1_result.get('processing_time_ms', 0)
            )
            
            # Only raise if it's a fatal error (not FloodWait)
            if 'FloodWait' in step1_result.get('error', ''):
                raise FloodWaitException(f"Step 1 FloodWait: {step1_result.get('error')}", 60)
            elif 'PasswordHashInvalid' in step1_result.get('error', ''):
                # Password already changed - mark as completed and continue
                logger.warning(f"Password already changed for {session_id}, marking step 1 as completed")
                await db.update_step_status(job_id, 1, 'completed')
                step_results['step1'] = {"success": True, "skipped": True, "reason": "Password already changed"}
            else:
                raise Exception(f"Step 1 failed: {step1_result.get('error')}")
    else:
        logger.info(f"Step 1 already completed for {session_id}, skipping")
        step_results['step1'] = {"success": True, "skipped": True}
    
    # ==================== STEP 2: Spam Check & Cleanup ====================
    if not step2_done:
        logger.info(f"Step 2: Spam check for {session_id}")
        await db.update_job_step(job_id, 2)
        await db.update_step_status(job_id, 2, 'processing')
        
        step2_result = await step2_spam_check(
            session_string=current_session_string,
            phone_number=payload['phone_number'],
            password=new_password,
            proxy=proxy,
            set_name=payload.get('set_name', random_name),
            clear_account=payload.get('clear_account', True),
            leave_chats=payload.get('leave_chats', True),
            block_bots=payload.get('block_bots', True),
            spam_check_required=payload.get('spam_check_required', True),
            set_username=payload.get('set_username'),
            set_profile_photo=payload.get('set_profile_photo'),
            set_bio=payload.get('set_bio')
        )
        
        if step2_result.get('success'):
            await db.update_step_status(job_id, 2, 'completed')
            await db.save_step_result(
                job_id, session_id, 2, "spam_check", "completed",
                result_data={
                    "spam_status": step2_result.get('spam_status'),
                    "cleared_status": step2_result.get('cleared_status'),
                    "total_processed": step2_result.get('total_processed'),
                    "left_chats": step2_result.get('left_chats'),
                    "blocked_bots": step2_result.get('blocked_bots'),
                    "profile_updated": step2_result.get('profile_updated'),
                    "updated_username": step2_result.get('updated_username')
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
        else:
            await db.update_step_status(job_id, 2, 'failed')
            await db.increment_step_retry_count(job_id, 2)
            await db.save_step_result(
                job_id, session_id, 2, "spam_check", "failed",
                result_data={"spam_status": step2_result.get('spam_status')},
                error_message=step2_result.get('error', f"Spam status: {step2_result.get('spam_status')}"),
                processing_time_ms=step2_result.get('processing_time_ms', 0)
            )
            raise Exception(f"Step 2 failed: {step2_result.get('error', step2_result.get('spam_status'))}")
    else:
        logger.info(f"Step 2 already completed for {session_id}, skipping")
        step_results['step2'] = {"success": True, "skipped": True}
    
    # ==================== STEP 3: Device Check ====================
    if not step3_done:
        logger.info(f"Step 3: Device check for {session_id}")
        await db.update_job_step(job_id, 3)
        await db.update_step_status(job_id, 3, 'processing')
        
        step3_result = await step3_device_check(
            session_string=current_session_string,
            phone_number=payload['phone_number'],
            proxy=proxy
        )
        
        if step3_result.get('success') and not step3_result.get('wait_required'):
            await db.update_step_status(job_id, 3, 'completed')
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
        elif step3_result.get('wait_required'):
            # Database থেকে সঠিক device_retry_count পড়ুন
            async with db.pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT device_retry_count FROM processing_jobs WHERE id = $1
                ''', job_id)
                device_retry_count = row['device_retry_count'] if row else 0
    
            next_retry = get_device_retry_time(device_retry_count)
    
            if next_retry and device_retry_count < 4:
                await db.schedule_device_retry(
                    job_id, device_retry_count + 1, next_retry,
                    "Device retry still waiting"
                )
                logger.info(f"Device retry {device_retry_count + 1} scheduled for {session_id}")
                
                await db.save_device_check(
                    session_id=session_id,
                    phone_number=payload['phone_number'],
                    total_devices=step3_result.get('total_devices', 0),
                    other_devices_terminated=step3_result.get('other_devices_terminated', 0),
                    termination_status=step3_result.get('device_termination_status', 'unknown'),
                    processing_time_ms=step3_result.get('processing_time_ms', 0),
                    retry_count=device_retry_count
                )
                
                await send_callback_to_main(
                    MAIN_SERVER_URL,
                    {
                        "session_id": session_id,
                        "overall_status": "waiting_device_retry",
                        "processor_url": PROCESSOR_SERVER_URL,
                        "current_step": 3,
                        "step3_result": step3_result,
                        "message": f"Device retry scheduled (attempt {device_retry_count + 1}/4)"
                    }
                )
                return
            else:
                # Max retries exceeded
                await db.update_step_status(job_id, 3, 'completed')
                await db.mark_job_completed(job_id)
                
                await db.save_device_check(
                    session_id=session_id,
                    phone_number=payload['phone_number'],
                    total_devices=step3_result.get('total_devices', 0),
                    other_devices_terminated=step3_result.get('other_devices_terminated', 0),
                    termination_status="max_retries_exceeded",
                    processing_time_ms=step3_result.get('processing_time_ms', 0),
                    retry_count=device_retry_count
                )
                
                final_payload = {
                    "session_id": session_id,
                    "overall_status": "completed_with_warning",
                    "processor_url": PROCESSOR_SERVER_URL,
                    "step1_result": step_results.get('step1'),
                    "step2_result": step_results.get('step2'),
                    "step3_result": step3_result,
                    "device_termination_status": "max_retries_exceeded",
                    "warning": "Device termination could not be completed after 4 retries"
                }
                
                await send_callback_to_main(MAIN_SERVER_URL, final_payload)
                return
        else:
            await db.update_step_status(job_id, 3, 'failed')
            await db.increment_step_retry_count(job_id, 3)
            await db.save_step_result(
                job_id, session_id, 3, "device_check", "failed",
                error_message=step3_result.get('error'),
                processing_time_ms=step3_result.get('processing_time_ms', 0)
            )
            raise Exception(f"Step 3 failed: {step3_result.get('error')}")
    else:
        logger.info(f"Step 3 already completed for {session_id}, skipping")
        step_results['step3'] = {"success": True, "skipped": True}
    
    # ==================== ALL STEPS COMPLETED ====================
    await db.mark_job_completed(job_id)
    
    final_payload = {
        "session_id": session_id,
        "overall_status": "completed",
        "processor_url": PROCESSOR_SERVER_URL,
        "step1_result": step_results.get('step1'),
        "step2_result": step_results.get('step2'),
        "step3_result": step_results.get('step3'),
    
        # নতুন data - Main Server-এ পাঠানোর জন্য
        "first_name": payload.get('first_name'),
        "last_name": payload.get('last_name'),
        "username": step_results.get('step2', {}).get('updated_username') or payload.get('username'),
        "profile_pic_url": payload.get('profile_pic_url') or payload.get('set_profile_photo'),
        "bio": payload.get('bio') or payload.get('set_bio'),
        "country_code": payload.get('country_code'),
        "country_name": payload.get('country_name'),
        "prefix": payload.get('prefix'),
        "price": payload.get('price'),
        "quality_score": payload.get('quality_score'),
        "profile_updated": step_results.get('step2', {}).get('profile_updated'),
    
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
    
    # process_device_retry এর শুরুতে
    use_proxy = payload.get('use_proxy', False)
    country_code = payload.get('country_code')
    proxy = None
    if use_proxy and country_code:
        proxy = await db.get_proxy_for_country(country_code)
        if not proxy:
            logger.warning(f"No active proxy available for country {country_code}")
    
    logger.info(f"🔄 Processing device retry for job {job_id}, session {session_id}")
    await db.mark_job_processing(job_id)
    
    try:
        # Get the most recent session string from payload
        session_string = payload.get('session_string', '')
        pw_row = None
        
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
            
            # Get password from password_changes if not in step1_result
            pw_row = await conn.fetchrow('''
                SELECT new_password FROM password_changes 
                WHERE session_id = $1 ORDER BY id DESC LIMIT 1
            ''', uuid.UUID(session_id))

        step1_result = json.loads(step1_row['result_data']) if step1_row and step1_row['result_data'] else {}
        step2_result = json.loads(step2_row['result_data']) if step2_row and step2_row['result_data'] else {}
        
        # Password missing হলে password_changes টেবিল থেকে আনুন
        if 'new_password' not in step1_result or not step1_result.get('new_password'):
            if pw_row:
                step1_result['new_password'] = pw_row['new_password']
        
        # Attempt device check again with proxy
        step3_result = await step3_device_check(
            session_string=session_string,
            phone_number=payload['phone_number'],
            proxy=proxy
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
        
        if step3_result.get('success') and not step3_result.get('wait_required'):
            # Device check succeeded on retry
            await db.mark_job_completed(job_id)
            await db.update_step_status(job_id, 3, 'completed')
            
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
                "processor_url": PROCESSOR_SERVER_URL,
                "step1_result": step1_result,
                "step2_result": step2_result,
                "step3_result": step3_result,
                "device_retry_success": True
            }
            
            await send_callback_to_main(MAIN_SERVER_URL, final_payload)
            logger.info(f"✅ Device retry succeeded for session {session_id}")
            
        elif step3_result.get('wait_required'):
            # Database থেকে সঠিক device_retry_count পড়ুন
            async with db.pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT device_retry_count FROM processing_jobs WHERE id = $1
                ''', job_id)
                device_retry_count = row['device_retry_count'] if row else 0
    
            next_retry = get_device_retry_time(device_retry_count)
    
            if next_retry and device_retry_count < 4:
                await db.schedule_device_retry(
                    job_id, device_retry_count + 1, next_retry,
                    "Device retry still waiting"
                )
                logger.info(f"Device retry {device_retry_count + 1} scheduled for {session_id}")
    
                # Waiting device retry callback পাঠান
                await send_callback_to_main(
                    MAIN_SERVER_URL,
                    {
                        "session_id": session_id,
                        "overall_status": "waiting_device_retry",
                        "processor_url": PROCESSOR_SERVER_URL,
                        "message": f"Device retry scheduled (attempt {device_retry_count + 1}/4)"
                    }
                )
            else:
                # Max retries exceeded
                await db.mark_job_completed(job_id)
                await db.update_step_status(job_id, 3, 'completed')
                
                final_payload = {
                    "session_id": session_id,
                    "overall_status": "completed_with_warning",
                    "processor_url": PROCESSOR_SERVER_URL,
                    "step1_result": step1_result,
                    "step2_result": step2_result,
                    "step3_result": step3_result,
                    "warning": "Device termination max retries exceeded"
                }
                
                await send_callback_to_main(MAIN_SERVER_URL, final_payload)
                
        else:
            raise Exception(f"Device retry failed: {step3_result.get('error')}")
            
    except FloodWaitException as e:
        # Handle FloodWait in device retry
        logger.warning(f"FloodWait in device retry for job {job_id}: {e}")
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT device_retry_count FROM processing_jobs WHERE id = $1
            ''', job_id)
            device_retry_count = row['device_retry_count'] if row else 0
        
        if device_retry_count < 4:
            next_retry = datetime.utcnow() + timedelta(seconds=e.retry_after)
            await db.schedule_device_retry(
                job_id, device_retry_count + 1, next_retry,
                f"FloodWait in device retry: {e.retry_after}s"
            )
            logger.info(f"Device retry {device_retry_count + 1} scheduled for {session_id}")
    
            # Waiting callback পাঠান
            await send_callback_to_main(
                MAIN_SERVER_URL,
                {
                    "session_id": session_id,
                    "overall_status": "waiting_device_retry",
                    "processor_url": PROCESSOR_SERVER_URL,
                    "message": f"FloodWait device retry scheduled (attempt {device_retry_count + 1}/4)"
                }
            )
        else:
            await db.mark_job_failed(job_id, f"Max retries exceeded with FloodWait: {e}")
    
            # Failure callback পাঠান
            await send_callback_to_main(
                MAIN_SERVER_URL,
                {
                    "session_id": session_id,
                    "overall_status": "failed",
                    "processor_url": PROCESSOR_SERVER_URL,
                    "processing_error": f"Max retries exceeded with FloodWait: {e}"
                }
            )
    
    except Exception as e:
        logger.error(f"Device retry failed for job {job_id}: {e}")
        await db.mark_job_failed(job_id, str(e))
    
        # Main Server-এ failure callback পাঠান
        await send_callback_to_main(
            MAIN_SERVER_URL,
            {
                "session_id": session_id,
                "overall_status": "failed",
                "processor_url": PROCESSOR_SERVER_URL,
                "processing_error": str(e)
            }
        )
    finally:
        if proxy:
            await db.release_proxy(proxy['hostname'], proxy['port'])

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
                except FloodWaitException as e:
                    logger.warning(f"Job {job['id']} hit FloodWait: {e}")
                    
                    retry_count = job.get('retry_count', 0) or 0
                    
                    if retry_count < MAX_RETRIES:
                        flood_wait_seconds = e.retry_after
                        next_retry = datetime.utcnow() + timedelta(seconds=flood_wait_seconds)
                        await db.schedule_job_retry(job['id'], retry_count + 1, next_retry, str(e))
                        logger.info(f"Job {job['id']} scheduled for FloodWait retry in {flood_wait_seconds}s")
                    else:
                        await db.mark_job_failed(job['id'], str(e))
                        await send_callback_to_main(
                            MAIN_SERVER_URL,
                            {
                                "session_id": str(job['session_id']),
                                "processor_url": PROCESSOR_SERVER_URL,
                                "overall_status": "failed",
                                "processing_error": str(e)
                            }
                        )
                        logger.error(f"Job {job['id']} marked as failed after {MAX_RETRIES} retries")
                        
                except Exception as e:
                    logger.error(f"Job {job['id']} failed: {e}")
                    
                    retry_count = job.get('retry_count', 0) or 0
                    
                    if retry_count < MAX_RETRIES:
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
                                "processor_url": PROCESSOR_SERVER_URL,
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
    version="3.0.1",
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
    
    # New fields
    set_profile_photo: Optional[str] = None
    set_bio: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    username: Optional[str] = None
    profile_pic_url: Optional[str] = None
    bio: Optional[str] = None
    use_proxy: bool = False
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    prefix: Optional[str] = None
    price: Optional[float] = None
    quality_score: Optional[int] = None
    new_password: Optional[str] = None
    
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
    step1_status: Optional[str] = None
    step2_status: Optional[str] = None
    step3_status: Optional[str] = None

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
                       updated_at, completed_at,
                       step1_status, step2_status, step3_status
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
                completed_at=row['completed_at'],
                step1_status=row['step1_status'],
                step2_status=row['step2_status'],
                step3_status=row['step3_status']
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
                           device_retry_count, created_at, updated_at, completed_at,
                           step1_status, step2_status, step3_status
                    FROM processing_jobs 
                    WHERE status = $1
                    ORDER BY created_at DESC 
                    LIMIT $2
                ''', status, limit)
            else:
                rows = await conn.fetch('''
                    SELECT id, session_id, status, current_step, retry_count,
                           device_retry_count, created_at, updated_at, completed_at,
                           step1_status, step2_status, step3_status
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
        "version": "3.0.1",
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
            "separate_device_retry_pool": True,
            "proxy_support": True
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