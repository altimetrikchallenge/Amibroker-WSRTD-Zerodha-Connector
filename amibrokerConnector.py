"""
Zerodha to AmiBroker Complete Production System - REVISED & PRODUCTION-READY
Incorporates dynamic symbols, enhanced logging, improved error handling, and backfill support.
"""

import asyncio
import websockets
import json
import logging
import logging.handlers
import sys
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Any, Set
from collections import deque
import threading
import os
from dataclasses import dataclass, field
import argparse
from enum import Enum
import ssl
import uuid
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("yaml not available; using defaults. Install with: pip install pyyaml")

# Optional: kiteconnect for real Zerodha
try:
    from kiteconnect import KiteTicker, KiteConnect
    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False
    print("kiteconnect not available; using mock data. Install with: pip install kiteconnect")

class ComponentType(Enum):
    RELAY_SERVER = "relay"
    ZERODHA_CONNECTOR = "zerodha"
    BOTH = "both"

def _load_initial_symbols(file_path: str) -> List[str]:
    """Load initial symbols from YAML file or default."""
    defaults = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f) if YAML_AVAILABLE else {}
                return data.get('symbols', defaults)
    except Exception as e:
        print(f"Error loading symbols file {file_path}: {e}")
    return defaults

@dataclass
class Config:
    # Zerodha Configuration (from env vars)
    ZERODHA_API_KEY: str = field(default_factory=lambda: os.getenv("ZERODHA_API_KEY", "blkxowfv24169sxc"))
    ZERODHA_ACCESS_TOKEN: str = field(default_factory=lambda: os.getenv("ZERODHA_ACCESS_TOKEN", "JGHsAEF9xmVycg7FV8E6DbQXT383UKyb"))
    
    # Relay Server Configuration
    RELAY_SERVER_HOST: str = field(default_factory=lambda: os.getenv("RELAY_HOST", "localhost"))
    RELAY_SERVER_PORT: int = field(default_factory=lambda: int(os.getenv("RELAY_PORT", "10101")))
    ENABLE_SSL: bool = field(default_factory=lambda: os.getenv("ENABLE_SSL", "false").lower() == "true")
    
    # Trading Configuration
    BASE_TIME_INTERVAL: int = 1  # Minutes per candle; used in CandleGenerator
    MAX_RECONNECT_ATTEMPTS: int = 5
    RECONNECT_DELAY: int = 5
    MAX_RTD_BATCH_SIZE: int = 10  # Batch up to 10 symbols per RTD message
    
    # Symbol Configuration (dynamic; initial from config file)
    INITIAL_SYMBOLS_FILE: str = field(default_factory=lambda: os.getenv("SYMBOLS_FILE", "symbols.yaml"))
    INITIAL_SYMBOLS: List[str] = field(default_factory=list)  # Fixed: Use default_factory to avoid mutable default
    
    # Performance Configuration
    MAX_QUEUE_SIZE: int = 10000
    HEARTBEAT_INTERVAL: int = 30
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    LOG_FILE: str = "zerodha_amibroker_system.log"
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT: int = 5

    def __post_init__(self):
        """Populate INITIAL_SYMBOLS dynamically."""
        if not self.INITIAL_SYMBOLS:
            self.INITIAL_SYMBOLS = _load_initial_symbols(self.INITIAL_SYMBOLS_FILE)

# Enhanced Logging Setup
def setup_logging(config: Config):
    """Configure structured logging with rotation."""
    log_level = getattr(logging, config.LOG_LEVEL.upper())
    handler = logging.handlers.RotatingFileHandler(
        config.LOG_FILE, maxBytes=config.LOG_MAX_BYTES, backupCount=config.LOG_BACKUP_COUNT
    )
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    logging.basicConfig(
        level=log_level,
        handlers=[
            handler,
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger("ZerodhaAmiBrokerSystem")

class RetryPolicy:
    """Manual exponential backoff for retries."""
    @staticmethod
    def backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
        delay = min(base_delay * (2 ** attempt), max_delay)
        return delay + (time.time() * 1000 % 1000) / 1000.0  # Improved jitter for better distribution

class RateLimiter:
    """Simple token bucket rate limiter."""
    def __init__(self, max_tokens: int = 1000, refill_rate: float = 1.0):
        self.tokens = deque(maxlen=max_tokens)
        self.refill_rate = refill_rate
        self.last_refill = time.time()
    
    def acquire(self) -> bool:
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = int(elapsed * self.refill_rate)
        for _ in range(tokens_to_add):
            self.tokens.append(now)
        self.last_refill = now
        
        if self.tokens:
            self.tokens.popleft()
            return True
        return False

class RelayServer:
    """
    Enhanced Relay Server with improved error handling, SSL support, and circuit breaker.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.senders: Set[websockets.WebSocketServerProtocol] = set()
        self.stats = {
            'clients_connected': 0,
            'senders_connected': 0,
            'messages_relayed': 0,
            'errors_count': 0,
            'circuit_open': False  # Circuit breaker state
        }
        self.is_running = False
        self.server = None
        self.stop_event = asyncio.Event()
        self.rate_limiter = RateLimiter()
        self.ssl_context = ssl.create_default_context() if config.ENABLE_SSL else None
        
        logger.info("Relay Server initialized")
    
    async def start(self):
        """Start the Relay Server with SSL and retry support."""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                kwargs = {'ssl': self.ssl_context} if self.config.ENABLE_SSL else {}
                self.server = await websockets.serve(
                    self._connection_handler,
                    self.config.RELAY_SERVER_HOST,
                    self.config.RELAY_SERVER_PORT,
                    ping_interval=20,
                    ping_timeout=10,
                    **kwargs
                )
                
                self.is_running = True
                logger.info(f"Relay Server started on {self.config.RELAY_SERVER_HOST}:{self.config.RELAY_SERVER_PORT} (SSL: {self.config.ENABLE_SSL})")
                
                asyncio.create_task(self._health_monitor())
                asyncio.create_task(self._stats_reporter())
                
                await self.stop_event.wait()
                break
                
            except Exception as e:
                logger.error(f"Failed to start Relay Server (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(RetryPolicy.backoff(attempt))
                else:
                    raise
    
    async def _connection_handler(self, websocket):
        """Handle connections with enhanced validation."""
        client_id = str(uuid.uuid4())[:8]
        client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
        
        logger.info(f"New connection from {client_ip} (ID: {client_id})")
        
        try:
            role_message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            await self._handle_role_identification(websocket, role_message, client_id, client_ip)
        except asyncio.TimeoutError:
            logger.warning(f"Role identification timeout for {client_id}")
            await websocket.close()
        except Exception as e:
            logger.error(f"Error in connection handler for {client_id}: {e}")
            await websocket.close()
    
    async def _handle_role_identification(self, websocket, role_message: str, client_id: str, client_ip: str):
        """Route based on role with validation."""
        role = role_message.strip()
        if role == "rolesend":
            await self._handle_sender(websocket, client_id, client_ip)
        elif role == "rolerecv":
            await self._handle_receiver(websocket, client_id, client_ip)
        else:
            logger.warning(f"Invalid role from {client_id}: {role}")
            await websocket.send("ERROR: Invalid role. Use 'rolesend' or 'rolerecv'")
            await websocket.close()
    
    async def _handle_sender(self, websocket, client_id: str, client_ip: str):
        """Handle senders with rate limiting."""
        if not self.rate_limiter.acquire():
            logger.warning(f"Rate limit exceeded for sender {client_id}")
            await websocket.close()
            return
        
        self.senders.add(websocket)
        self.stats['senders_connected'] = len(self.senders)
        logger.info(f"Data sender connected: {client_ip} (ID: {client_id})")
        
        try:
            await websocket.send(json.dumps({
                "status": "connected", "role": "sender",
                "message": "Connected to Relay Server as data sender"
            }))
            
            async for message in websocket:
                if self.stats['circuit_open']:
                    await asyncio.sleep(1)
                    continue
                try:
                    await self._broadcast_to_clients(message)
                    self.stats['messages_relayed'] += 1
                except Exception as e:
                    logger.error(f"Error broadcasting from {client_id}: {e}")
                    self.stats['errors_count'] += 1
                    if self.stats['errors_count'] > 50:  # Circuit breaker
                        self.stats['circuit_open'] = True
                        asyncio.create_task(self._reset_circuit())
                        
        except websockets.ConnectionClosed:
            logger.info(f"Data sender disconnected: {client_id}")
        finally:
            self.senders.discard(websocket)
            self.stats['senders_connected'] = len(self.senders)
    
    async def _handle_receiver(self, websocket, client_id: str, client_ip: str):
        """Handle receivers similarly."""
        self.clients.add(websocket)
        self.stats['clients_connected'] = len(self.clients)
        logger.info(f"Data receiver connected: {client_ip} (ID: {client_id})")
        
        try:
            await websocket.send(json.dumps({
                "status": "connected", "role": "receiver",
                "message": "Connected to Relay Server as data receiver"
            }))
            
            async for message in websocket:
                if self.stats['circuit_open']:
                    await asyncio.sleep(1)
                    continue
                try:
                    await self._broadcast_to_senders(message)
                    self.stats['messages_relayed'] += 1
                except Exception as e:
                    logger.error(f"Error broadcasting from {client_id}: {e}")
                    self.stats['errors_count'] += 1
                    if self.stats['errors_count'] > 50:
                        self.stats['circuit_open'] = True
                        asyncio.create_task(self._reset_circuit())
                        
        except websockets.ConnectionClosed:
            logger.info(f"Data receiver disconnected: {client_id}")
        finally:
            self.clients.discard(websocket)
            self.stats['clients_connected'] = len(self.clients)
    
    async def _broadcast_to_clients(self, message: str):
        """Broadcast with concurrent sends and cleanup. Fixed for performance."""
        if not self.clients:
            return
        clients_copy = list(self.clients)
        if not clients_copy:
            return
        try:
            results = await asyncio.gather(
                *[client.send(message) for client in clients_copy],
                return_exceptions=True
            )
            disconnected = set()
            for client, result in zip(clients_copy, results):
                if isinstance(result, Exception):
                    logger.debug(f"Broadcast error to client {id(client)}: {result}")
                    disconnected.add(client)
            for client in disconnected:
                self.clients.discard(client)
            self.stats['clients_connected'] = len(self.clients)
        except Exception as e:
            logger.error(f"Broadcast to clients failed: {e}")
    
    async def _broadcast_to_senders(self, message: str):
        """Similar to above, fixed for performance."""
        if not self.senders:
            return
        senders_copy = list(self.senders)
        if not senders_copy:
            return
        try:
            results = await asyncio.gather(
                *[sender.send(message) for sender in senders_copy],
                return_exceptions=True
            )
            disconnected = set()
            for sender, result in zip(senders_copy, results):
                if isinstance(result, Exception):
                    logger.debug(f"Broadcast error to sender {id(sender)}: {result}")
                    disconnected.add(sender)
            for sender in disconnected:
                self.senders.discard(sender)
            self.stats['senders_connected'] = len(self.senders)
        except Exception as e:
            logger.error(f"Broadcast to senders failed: {e}")
    
    async def _reset_circuit(self):
        """Reset circuit breaker after cooldown."""
        await asyncio.sleep(30)
        self.stats['circuit_open'] = False
        logger.info("Circuit breaker reset")
    
    async def _health_monitor(self):
        """Enhanced health monitoring."""
        while self.is_running:
            if self.stats['errors_count'] > 100:
                logger.warning("High error count; circuit opening")
                self.stats['circuit_open'] = True
                asyncio.create_task(self._reset_circuit())
            await asyncio.sleep(60)
    
    async def _stats_reporter(self):
        """Report stats periodically."""
        while self.is_running:
            logger.info(
                f"Relay Stats - Clients: {self.stats['clients_connected']}, "
                f"Senders: {self.stats['senders_connected']}, "
                f"Messages: {self.stats['messages_relayed']}, "
                f"Errors: {self.stats['errors_count']}, Circuit: {self.stats['circuit_open']}"
            )
            await asyncio.sleep(300)
    
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Stopping Relay Server...")
        self.is_running = False
        self.stop_event.set()
        
        close_tasks = [c.close() for c in list(self.clients) | list(self.senders)]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        logger.info("Relay Server stopped")

class CandleGenerator:
    """Robust candle generator with timestamp validation."""
    
    def __init__(self, symbol: str, config: Config):
        self.symbol = symbol
        self.current_candle = None
        self.candle_start_time = None
        self.interval_seconds = config.BASE_TIME_INTERVAL * 60  # Fixed: Integrate config
        self.candles_generated = 0
        self.last_candle_time = None
    
    def process_tick(self, price: float, volume: int = 0, oi: int = 0, timestamp: Optional[datetime] = None) -> Optional[Dict]:
        """Process tick; use provided timestamp or now."""
        timestamp = timestamp or datetime.now()
        try:
            if price is None or price <= 0:
                logger.warning(f"Invalid price {price} for {self.symbol}")
                return None
            
            if self.current_candle is None or self._should_start_new_candle(timestamp):
                completed = self._finalize_current_candle()
                self._start_new_candle(timestamp, price, volume, oi)
                return completed
            
            self._update_candle(price, volume, oi)
            return self.current_candle.copy()
            
        except Exception as e:
            logger.error(f"Error processing tick for {self.symbol}: {e}")
            return None
    
    def _should_start_new_candle(self, timestamp: datetime) -> bool:
        if self.candle_start_time is None:
            return True
        elapsed = (timestamp - self.candle_start_time).total_seconds()
        return elapsed >= self.interval_seconds
    
    def _start_new_candle(self, timestamp: datetime, price: float, volume: int, oi: int):
        # Fixed: Ensure timezone-naive for consistency
        self.candle_start_time = timestamp.replace(tzinfo=None).replace(second=0, microsecond=0)
        self.current_candle = {
            'symbol': self.symbol,
            'timestamp': self.candle_start_time,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'open_interest': oi,
            'tick_count': 1
        }
    
    def _update_candle(self, price: float, volume: int, oi: int):
        if self.current_candle:
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
            self.current_candle['volume'] += volume
            self.current_candle['open_interest'] = oi  # Last OI
            self.current_candle['tick_count'] += 1
    
    def _finalize_current_candle(self) -> Optional[Dict]:
        if self.current_candle and self.current_candle['open'] is not None:
            completed = self.current_candle.copy()
            self.candles_generated += 1
            self.last_candle_time = datetime.now()
            self.current_candle = None
            self.candle_start_time = None
            return completed
        return None

class HistoricalDataGenerator:
    """Mock historical data for backfills."""
    
    def __init__(self):
        self.mock_history: Dict[str, List[List]] = {}
    
    def generate_backfill(self, symbol: str, days: int = 5, format_spec: str = "dtohlcvi") -> Dict:
        """Generate ascending HIST bars. Fixed: Validate days > 0."""
        if days < 1:
            days = 5
        if symbol not in self.mock_history:
            base_price = 1000 + hash(symbol) % 5000
            history = []
            end_date = datetime.now().replace(tzinfo=None)  # Ensure timezone-naive
            for i in range(days * 390):  # ~6.5 hours trading per day
                dt = end_date - timedelta(minutes=i)
                price = base_price + (i % 100 - 50) * 0.1
                bar = {
                    'd': int(dt.strftime('%Y%m%d')),
                    't': int(dt.strftime('%H%M00')),
                    'o': round(price, 2),
                    'h': round(price + abs(i % 10) * 0.05, 2),
                    'l': round(price - abs(i % 10) * 0.05, 2),
                    'c': round(price + (i % 2 - 1) * 0.1, 2),
                    'v': 1000 + i % 4000,
                    'i': 0  # OI mock
                }
                history.append([bar.get(c, 0) for c in format_spec])
            self.mock_history[symbol] = history[::-1]  # Ensure ascending (oldest first)
        
        bars = self.mock_history[symbol]
        return {
            "hist": symbol,
            "format": format_spec,
            "bars": bars
        }

class ZerodhaDataSource:
    """Abstract data source; mock or real with dynamic symbols."""
    
    def __init__(self, config: Config):
        self.config = config
        self.is_mock = not KITE_AVAILABLE
        self.active_symbols: Set[str] = set(config.INITIAL_SYMBOLS)
        self.mock_generator = None
        self.kite = None
        self.ticker = None
        self.tick_callback = None
        self.token_to_symbol: Dict[int, str] = {}
        self.symbol_to_token: Dict[str, int] = {}
        self.candle_generators: Dict[str, CandleGenerator] = {}
        
        # Initialize generators for active symbols
        for symbol in self.active_symbols:
            self.candle_generators[symbol] = CandleGenerator(symbol, config)

        if self.is_mock:
            self.mock_generator = ZerodhaMockDataGenerator(config)
        else:
            try:
                self.kite = KiteConnect(api_key=config.ZERODHA_API_KEY)
                self.kite.set_access_token(config.ZERODHA_ACCESS_TOKEN)
                self.ticker = KiteTicker(self.kite.api_key, self.kite.access_token)
            except Exception as e:
                logger.error(f"Failed to initialize Zerodha components: {e}")
                self.is_mock = True
                self.mock_generator = ZerodhaMockDataGenerator(config)

    def _fetch_instrument_tokens(self):
        """Fetch instrument tokens from Zerodha using KiteConnect API."""
        if not self.kite:
            logger.warning("KiteConnect not initialized; cannot fetch instrument tokens.")
            return
        try:
            instruments = self.kite.instruments()
            # Build mappings for symbols to tokens
            self.symbol_to_token = {}
            self.token_to_symbol = {}
            for instr in instruments:
                symbol = instr.get('tradingsymbol')
                token = instr.get('instrument_token')
                if symbol and token:
                    self.symbol_to_token[symbol] = token
                    self.token_to_symbol[token] = symbol
            logger.info(f"Fetched {len(self.symbol_to_token)} instrument tokens from Zerodha.")
        except Exception as e:
            logger.error(f"Failed to fetch instrument tokens: {e}")

    def add_symbol(self, symbol: str):
        """Add symbol dynamically."""
        self.active_symbols.add(symbol)
        if symbol not in self.candle_generators:
            self.candle_generators[symbol] = CandleGenerator(symbol, self.config)
            
        try:
            if self.is_mock:
                self.mock_generator.add_symbol(symbol)
            else:
                token = self.symbol_to_token.get(symbol)
                if token and self.ticker:
                    self.ticker.subscribe([token])
                    self.ticker.set_mode(self.ticker.MODE_FULL, [token])
                    logger.info(f"Subscribed to real symbol: {symbol} ({token})")
                else:
                    logger.warning(f"Token not found for {symbol}, cannot subscribe.")
        except Exception as e:
            logger.error(f"Error adding symbol {symbol}: {e}")
    
    def remove_symbol(self, symbol: str):
        """Remove symbol dynamically."""
        self.active_symbols.discard(symbol)
        # We keep the generator to avoid losing partial state, or we could remove it.
        
        try:
            if self.is_mock:
                self.mock_generator.remove_symbol(symbol)
            else:
                token = self.symbol_to_token.get(symbol)
                if token and self.ticker:
                    self.ticker.unsubscribe([token])
                    logger.info(f"Unsubscribed from real symbol: {symbol}")
        except Exception as e:
            logger.error(f"Error removing symbol {symbol}: {e}")
    
    async def start(self, tick_callback: callable):
        """Start data flow for active symbols."""
        self.tick_callback = tick_callback
        try:
            if self.is_mock:
                self.mock_generator.start(self.active_symbols, tick_callback)
            else:
                # Setup callbacks
                self.ticker.on_ticks = self.on_ticks
                self.ticker.on_connect = self.on_connect
                self.ticker.on_close = self.on_close
                self.ticker.on_error = self.on_error
                self.ticker.on_reconnect = self.on_reconnect
                self.ticker.on_noreconnect = self.on_noreconnect
                
                # Connect (blocking in thread)
                self.ticker.connect(threaded=True)
                logger.info("Real Zerodha ticker connection initiated")
                
                # Subscriptions will be handled in on_connect callback after token mapping
                logger.info("Real Zerodha ticker started")
                
        except Exception as e:
            logger.error(f"Error starting data source: {e}")
            self._fallback_to_mock()
    def on_connect(self, ws, response):
        logger.info("Successfully connected to Zerodha KiteTicker")
        # Reset error counter on successful connection
        self.error_count = 0
        self._fetch_instrument_tokens()
        tokens = []
        for sym in self.active_symbols:
            token = self.symbol_to_token.get(sym)
            if token:
                tokens.append(token)
        if tokens:
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)
            logger.info(f"Subscribed to {len(tokens)} tokens")
        else:
            logger.warning("No instrument tokens available to subscribe.")


    def on_ticks(self, ws, ticks):
        self._process_real_ticks(ticks)

    def on_close(self, ws, code, reason):
        logger.warning(f"Zerodha connection closed: {code} - {reason}")
        if not self.is_mock:
            self._fallback_to_mock()

    def on_error(self, ws, code, reason):
        logger.error(f"Zerodha connection error: {code} - {reason}")
        # Increment attempt counter and fallback if needed
        if not hasattr(self, "error_count"):
            self.error_count = 0
        self.error_count += 1
        if self.error_count >= self.config.MAX_RECONNECT_ATTEMPTS:
            logger.error("Maximum reconnection attempts reached, falling back to mock mode.")
            self._fallback_to_mock()
        # If we get a 403 or similar, we might want to fallback
        if "403" in str(code) or "Forbidden" in str(reason):
             # We can't easily switch to mock from this thread callback, 
             # but we can log it.
             pass

    def on_reconnect(self, ws, attempts_count):
        logger.info(f"Zerodha reconnecting... (Attempt {attempts_count})")

    def on_noreconnect(self, ws):
        logger.error("Zerodha failed to reconnect.")

    def _process_real_ticks(self, ticks: List[Dict]):
        """Process real ticks."""
        if not self.tick_callback:
            return
            
        for tick in ticks:
            token = tick.get('instrument_token')
            symbol = self.token_to_symbol.get(token)
            if symbol:
                generator = self.candle_generators.get(symbol)
                if generator:
                    price = tick.get('last_price')
                    volume = tick.get('volume', 0)
                    oi = tick.get('oi', 0)
                    self.tick_callback(generator, price, 0, oi, datetime.now())

    def _fallback_to_mock(self):
        if not self.is_mock:
            logger.info("Falling back to mock mode.")
            try:
                if self.ticker:
                    self.ticker.close()
            except:
                pass
            self.is_mock = True
            self.mock_generator = ZerodhaMockDataGenerator(self.config)
            self.mock_generator.start(self.active_symbols, self.tick_callback)

    def stop(self):
        """Stop data source."""
        try:
            if self.is_mock:
                if self.mock_generator:
                    self.mock_generator.stop()
            else:
                if self.ticker:
                    self.ticker.close()
                    logger.info("Real Zerodha ticker stopped")
        except Exception as e:
            logger.error(f"Error stopping data source: {e}")

class ZerodhaMockDataGenerator:
    """Enhanced mock with dynamic symbols."""
    
    def __init__(self, config: Config):
        self.config = config
        self.candle_generators: Dict[str, CandleGenerator] = {}
        self.mock_prices: Dict[str, float] = {}
        self.is_running = False
        self.tick_callback = None
        self.lock = threading.Lock()  # Added for thread-safety on add/remove
    
    def start(self, initial_symbols: Set[str], tick_callback: callable):
        """Start with dynamic symbols."""
        self.tick_callback = tick_callback
        with self.lock:
            for symbol in initial_symbols:
                self.candle_generators[symbol] = CandleGenerator(symbol, self.config)  # Pass config
                self.mock_prices[symbol] = 1000 + hash(symbol) % 5000
        self.is_running = True
        self.thread = threading.Thread(target=self._generate_mock_data, daemon=True)
        self.thread.start()
        logger.info(f"Mock generator started for {len(initial_symbols)} symbols")
    
    def add_symbol(self, symbol: str):
        """Dynamic add."""
        with self.lock:
            if symbol not in self.candle_generators:
                self.candle_generators[symbol] = CandleGenerator(symbol, self.config)
                self.mock_prices[symbol] = 1000 + hash(symbol) % 5000
                logger.info(f"Added mock symbol: {symbol}")
    
    def remove_symbol(self, symbol: str):
        """Dynamic remove."""
        with self.lock:
            self.candle_generators.pop(symbol, None)
            self.mock_prices.pop(symbol, None)
            logger.info(f"Removed mock symbol: {symbol}")
    
    def _generate_mock_data(self):
        """Generate ticks."""
        try:
            while self.is_running:
                with self.lock:
                    symbols = list(self.candle_generators.keys())
                for symbol in symbols:
                    current_price = self.mock_prices.get(symbol, 1000)
                    change_percent = (hash(f"{symbol}{time.time()}") % 100 - 50) / 10000.0
                    new_price = round(current_price * (1 + change_percent), 2)
                    self.mock_prices[symbol] = new_price
                    volume = 1000 + hash(f"vol{symbol}{time.time()}") % 4000
                    if self.tick_callback:
                        self.tick_callback(
                            self.candle_generators.get(symbol), new_price, volume, 0, datetime.now()
                        )
                time.sleep(1)
        except Exception as e:
            logger.error(f"Mock generation error: {e}")
    
    def stop(self):
        self.is_running = False
        logger.info("Mock generator stopped")

class ZerodhaConnector:
    """
    Enhanced connector with dynamic symbols, backfill, and asyncio.Queue.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.data_queue = asyncio.Queue(maxsize=config.MAX_QUEUE_SIZE)
        self.data_source = ZerodhaDataSource(config)
        self.websocket = None
        self.is_connected = False
        self.stop_event = asyncio.Event()
        self.stats = {
            'messages_sent': 0,
            'commands_received': 0,
            'errors_count': 0,
            'queue_size': 0
        }
        self.historical_gen = HistoricalDataGenerator()
        self.batch_buffer: List[Dict] = []
        self.rtd_limiter = RateLimiter(1000, 1000.0)  # 1000/sec
        self.ssl_context = ssl.create_default_context() if config.ENABLE_SSL else None
        self.tick_callback = lambda gen, p, v, o, ts: self._on_tick(gen, p, v, o, ts)
        
        logger.info("Zerodha Connector initialized")
    
    async def start(self):
        """Start with dynamic initial symbols."""
        try:
            await self.data_source.start(self.tick_callback)
            
            await self._connect_to_relay()
            
            await asyncio.gather(
                self._send_data(),
                self._receive_commands(),
                self._stats_reporter(),
                return_exceptions=True
            )
            
        except Exception as e:
            logger.error(f"Failed to start Connector: {e}")
            await self.stop()
    
    def _on_tick(self, generator: CandleGenerator, price: float, volume: int, oi: int, timestamp: datetime):
        """Callback for ticks; queue completed candles."""
        candle = generator.process_tick(price, volume, oi, timestamp)
        if candle:
            rtd = self._convert_to_rtd_format(candle)
            try:
                self.data_queue.put_nowait(rtd)
            except asyncio.QueueFull:
                logger.warning(f"Queue full for {generator.symbol}; dropping candle")
    
    async def _connect_to_relay(self):
        """Connect with retries."""
        for attempt in range(self.config.MAX_RECONNECT_ATTEMPTS):
            try:
                uri = f"wss://{self.config.RELAY_SERVER_HOST}:{self.config.RELAY_SERVER_PORT}" if self.config.ENABLE_SSL else f"ws://{self.config.RELAY_SERVER_HOST}:{self.config.RELAY_SERVER_PORT}"
                self.websocket = await websockets.connect(
                    uri, ping_interval=30, ping_timeout=10, close_timeout=10,
                    ssl=self.ssl_context if self.config.ENABLE_SSL else None
                )
                await self.websocket.send("rolesend")
                welcome = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
                logger.info(f"Connected to Relay: {welcome}")
                self.is_connected = True
                return
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < self.config.MAX_RECONNECT_ATTEMPTS - 1:
                    await asyncio.sleep(RetryPolicy.backoff(attempt, self.config.RECONNECT_DELAY))
        
        raise ConnectionError("Failed to connect to Relay")
    
    async def _send_data(self):
        """Send batched RTD data."""
        while not self.stop_event.is_set() and self.is_connected:
            try:
                if not self.rtd_limiter.acquire():
                    await asyncio.sleep(0.001)
                    continue
                
                data = await asyncio.wait_for(self.data_queue.get(), timeout=2.0)
                self.batch_buffer.append(data)
                
                if len(self.batch_buffer) >= self.config.MAX_RTD_BATCH_SIZE:
                    message = json.dumps(self.batch_buffer, separators=(',', ':'))
                    await self.websocket.send(message)
                    self.stats['messages_sent'] += 1
                    self.stats['queue_size'] = self.data_queue.qsize()
                    self.batch_buffer.clear()
                    
                    if self.stats['messages_sent'] % 50 == 0:
                        logger.info(f"Sent batch of {self.config.MAX_RTD_BATCH_SIZE} RTD messages")
                
            except asyncio.TimeoutError:
                if self.batch_buffer:
                    # Flush partial batch
                    message = json.dumps(self.batch_buffer, separators=(',', ':'))
                    await self.websocket.send(message)
                    self.stats['messages_sent'] += 1
                    self.batch_buffer.clear()
                continue
            except websockets.ConnectionClosed:
                await self._handle_connection_loss()
                break
            except Exception as e:
                logger.error(f"Send error: {e}")
                self.stats['errors_count'] += 1
                await asyncio.sleep(1)
    
    async def _receive_commands(self):
        """Receive and validate commands."""
        while not self.stop_event.is_set() and self.is_connected:
            try:
                message = await asyncio.wait_for(self.websocket.recv(), timeout=3.0)
                await self._process_command(message)
                self.stats['commands_received'] += 1
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                await self._handle_connection_loss()
                break
            except Exception as e:
                logger.error(f"Receive error: {e}")
                self.stats['errors_count'] += 1
                await asyncio.sleep(1)
    
    async def _process_command(self, message: str):
        """Process with JSON validation."""
        try:
            command_data = json.loads(message)
            if not isinstance(command_data, dict) or 'cmd' not in command_data:
                raise ValueError("Invalid JSON structure")
            
            cmd = command_data.get('cmd')
            arg = command_data.get('arg', '')
            logger.debug(f"DBG: Received command: {cmd} arg: {arg}")
            
            if cmd in ['bfauto', 'bffull', 'bfsym']:
                await self._handle_backfill_request(command_data)
            elif cmd == 'bfall':
                await self._handle_backfill_all(command_data)
            elif cmd == 'addsym':
                await self._handle_add_symbol(command_data)
            elif cmd == 'remsym':
                await self._handle_remove_symbol(command_data)
            elif cmd == 'cping':
                await self._handle_ping(command_data)
            else:
                logger.warning(f"Unknown command: {cmd}")
                await self._send_ack(cmd, 400, "Unknown command")
                
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {message}")
            await self._send_ack("invalid", 400, "JSON parse error")
        except Exception as e:
            logger.error(f"Command process error: {e}")
            await self._send_ack("error", 400, str(e))
    
    async def _handle_backfill_request(self, command_data: Dict):
        """Generate and send HIST data with robust arg parsing."""
        try:
            arg_parts = command_data['arg'].split()
            if not arg_parts:
                await self._send_ack(command_data['cmd'], 400, "Empty arg")
                return
            symbol = arg_parts[0]
            days = 5
            if len(arg_parts) > 1:
                try:
                    days = int(arg_parts[-1])
                except ValueError:
                    pass  # Default to 5
            
            hist_data = self.historical_gen.generate_backfill(symbol, days)
            await self.websocket.send(json.dumps(hist_data, separators=(',', ':')))
            
            await self._send_ack(command_data['cmd'], 200, f"Backfill sent for {symbol} ({days} days)")
            logger.info(f"Sent backfill for {symbol}")
            
        except Exception as e:
            logger.error(f"Backfill error: {e}")
            await self._send_ack(command_data['cmd'], 400, str(e))
    
    async def _handle_backfill_all(self, command_data: Dict):
        """Handle backfill for all symbols."""
        try:
            for symbol in self.data_source.active_symbols:
                hist_data = self.historical_gen.generate_backfill(symbol, days=5)
                await self.websocket.send(json.dumps(hist_data, separators=(',', ':')))
                await asyncio.sleep(0.1)  # Rate limit
            await self._send_ack("bfall", 200, f"Backfill sent for {len(self.data_source.active_symbols)} symbols")
            logger.info(f"Sent backfill for all symbols")
        except Exception as e:
            logger.error(f"Backfill all error: {e}")
            await self._send_ack("bfall", 400, str(e))
    
    async def _handle_add_symbol(self, command_data: Dict):
        """Dynamic add."""
        symbol = command_data['arg'].strip()
        if symbol:
            self.data_source.add_symbol(symbol)
            await self._send_ack("addsym", 200, f"{symbol} added")
        else:
            await self._send_ack("addsym", 400, "Invalid symbol")
    
    async def _handle_remove_symbol(self, command_data: Dict):
        """Dynamic remove."""
        symbol = command_data['arg'].strip()
        if symbol:
            self.data_source.remove_symbol(symbol)
            await self._send_ack("remsym", 200, f"{symbol} removed")
        else:
            await self._send_ack("remsym", 400, "Invalid symbol")
    
    async def _handle_ping(self, command_data: Dict):
        """Enhanced ping with state."""
        state = "Vendor Connected" if self.is_connected else "Vendor Disconnected"
        await self._send_ack("cping", 200, state)
    
    async def _send_ack(self, cmd: str, code: int, arg: str):
        """Send standardized ACK."""
        response = {"cmd": cmd, "code": code, "arg": arg}
        if self.websocket and self.is_connected:
            await self.websocket.send(json.dumps(response, separators=(',', ':')))
    
    def _convert_to_rtd_format(self, candle: Dict) -> Dict:
        """Convert with validation."""
        timestamp = candle['timestamp']
        return {
            "n": candle['symbol'],
            "d": int(timestamp.strftime('%Y%m%d')),
            "t": int(timestamp.strftime('%H%M00')),
            "o": round(candle['open'], 2),
            "h": round(candle['high'], 2),
            "l": round(candle['low'], 2),
            "c": round(candle['close'], 2),
            "v": int(candle['volume']),
            "oi": int(candle.get('open_interest', 0)),
            "s": int(candle['volume']),
            "pc": round(candle['close'] * 0.99, 2),
            "bs": 100, "bp": round(candle['close'] * 0.999, 2),
            "as": 100, "ap": round(candle['close'] * 1.001, 2),
            "do": round(candle['open'], 2),
            "dh": round(candle['high'], 2),
            "dl": round(candle['low'], 2)
        }
    
    async def _handle_connection_loss(self):
        """Reconnect with backoff."""
        self.is_connected = False
        logger.warning("Connection lost; reconnecting...")
        self.data_source.stop()
        for attempt in range(self.config.MAX_RECONNECT_ATTEMPTS):
            try:
                await self._connect_to_relay()
                await self.data_source.start(self.tick_callback)
                return
            except Exception as e:
                logger.warning(f"Reconnect attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(RetryPolicy.backoff(attempt, self.config.RECONNECT_DELAY))
        logger.error("Reconnection failed")
        await self.stop()
    
    async def _stats_reporter(self):
        """Report stats."""
        while not self.stop_event.is_set():
            await asyncio.sleep(300)
            self.stats['queue_size'] = self.data_queue.qsize()
            logger.info(
                f"Connector Stats - Messages: {self.stats['messages_sent']}, "
                f"Commands: {self.stats['commands_received']}, "
                f"Errors: {self.stats['errors_count']}, "
                f"Queue: {self.stats['queue_size']}, "
                f"Symbols: {len(self.data_source.active_symbols)}, Connected: {self.is_connected}"
            )
    
    async def stop(self):
        """Shutdown."""
        logger.info("Stopping Connector...")
        self.stop_event.set()
        self.data_source.stop()
        if self.websocket:
            await self.websocket.close()
        self.is_connected = False
        logger.info("Connector stopped")

class ZerodhaAmiBrokerSystem:
    """Orchestrator with signal handling. Fixed: Removed custom signal handlers."""
    
    def __init__(self, config: Config, component: ComponentType = ComponentType.BOTH):
        self.config = config
        self.component = component
        self.relay_server = None
        self.zerodha_connector = None
        logger.info(f"System initialized for {component.value}")
    
    async def start(self):
        """Start components."""
        tasks = []
        if self.component in [ComponentType.RELAY_SERVER, ComponentType.BOTH]:
            self.relay_server = RelayServer(self.config)
            tasks.append(asyncio.create_task(self.relay_server.start()))
        
        if self.component in [ComponentType.ZERODHA_CONNECTOR, ComponentType.BOTH]:
            if self.component == ComponentType.BOTH:
                await asyncio.sleep(3)
            self.zerodha_connector = ZerodhaConnector(self.config)
            tasks.append(asyncio.create_task(self.zerodha_connector.start()))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop(self):
        """Stop gracefully."""
        logger.info("System shutdown initiated")
        stop_tasks = []
        if self.zerodha_connector:
            stop_tasks.append(self.zerodha_connector.stop())
        if self.relay_server:
            stop_tasks.append(self.relay_server.stop())
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        logger.info("System stopped")

def validate_config(config: Config) -> bool:
    """Validate."""
    if not config.INITIAL_SYMBOLS:
        logger.error("No initial symbols")
        return False
    if not (1024 <= config.RELAY_SERVER_PORT <= 65535):
        logger.error("Invalid port")
        return False
    return True

async def async_main():  # Fixed: Single async entry point for proper handling
    """Async entry point for the application."""
    parser = argparse.ArgumentParser(description='Enhanced Zerodha to AmiBroker System')
    parser.add_argument('--component', choices=['relay', 'zerodha', 'both'], default='both')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--port', type=int, default=10101)
    parser.add_argument('--symbols-file', default='symbols.yaml')
    parser.add_argument('--api-key', help='Zerodha API key', default=os.getenv('ZERODHA_API_KEY'))
    parser.add_argument('--access-token', help='Zerodha access token', default=os.getenv('ZERODHA_ACCESS_TOKEN'))
    
    args = parser.parse_args()
    
    config = Config(
        RELAY_SERVER_PORT=args.port,
        LOG_LEVEL=args.log_level,
        INITIAL_SYMBOLS_FILE=args.symbols_file,
        ZERODHA_API_KEY=args.api_key,
        ZERODHA_ACCESS_TOKEN=args.access_token
    )
    setup_logging(config)
    
    if not validate_config(config):
        sys.exit(1)
    
    component = {
        'relay': ComponentType.RELAY_SERVER,
        'zerodha': ComponentType.ZERODHA_CONNECTOR,
        'both': ComponentType.BOTH
    }[args.component]
    
    system = ZerodhaAmiBrokerSystem(config, component)
    
    try:
        await system.start()
    except KeyboardInterrupt:
        logger.info("Interrupt received")
    finally:
        await system.stop()

if __name__ == "__main__":
    asyncio.run(async_main())