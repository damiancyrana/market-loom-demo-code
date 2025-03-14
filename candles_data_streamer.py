class CandlesDataStreamer:
    def __init__(self, market_loom_instance, session_id, symbols):
        self.market_loom_instance = market_loom_instance
        self.session_id = session_id
        self.symbols = symbols
        self.symbols_set = set(symbols)
        self.symbols_count = len(symbols)
        self.socket_lock = threading.Lock()
        
        # System configuration
        if hasattr(market_loom_instance, 'system_params'):
            self.system_params = market_loom_instance.system_params
        else:
            self.system_params = SystemParameterCalculator.calculate_parameters(
                self.symbols_count)
            
        # Enhanced buffering parameters
        self.batch_size_threshold = min(
            100, self.system_params.get('candle_batch_size', 500))
        self.batch_time_threshold = min(
            0.5, self.system_params.get('candle_batch_time', 0.2))
        self.stream_buffer_size = max(
            65536, self.system_params.get('candle_stream_buffer_size', 32768))
        
        # Binary buffer instead of string
        self.buffer = bytearray(self.stream_buffer_size)
        self.buffer_pos = 0
        
        # Performance monitoring
        self.performance = {
            'candles_received': 0,
            'batches_processed': 0,
            'buffer_high_water': 0,
            'empty_receives': 0,
            'last_empty_receive': 0,
            'reconnect_attempts': 0,
            'processing_times': deque(maxlen=20),
            'last_report_time': time.time(),
            'last_market_closure_notification': 0
        }
        
        # Subscription status
        self.subscribed_symbols = set()
        self.subscription_attempts = {symbol: 0 for symbol in symbols}
        self.last_candle_time = {symbol: 0 for symbol in symbols}
        
        # Logger and functions
        self.logger_info = logger.info
        self.logger_error = logger.error
        self.logger_warning = logger.warning
        self.logger_critical = logger.critical
        self.telegram_send = telegram_notifier.send_message
        
        # Performance monitoring thread
        threading.Thread(target=self._monitor_performance, daemon=True).start()
    
    def _monitor_performance(self):
        """Periodically monitors performance and reports statistics"""
        while True:
            try:
                time.sleep(300)  # Every 5 minutes
                current_time = time.time()
                elapsed = current_time - self.performance['last_report_time']
                
                if elapsed > 0:
                    candles_per_second = self.performance['candles_received'] / elapsed
                    batches_per_second = self.performance['batches_processed'] / elapsed
                    
                    avg_processing_time = 0
                    if self.performance['processing_times']:
                        avg_processing_time = sum(
                            self.performance['processing_times']
                        ) / len(self.performance['processing_times'])
                    
                    self.logger_info(
                        f"CandlesDataStreamer Statistics (5 min):\n"
                        f"- Candles received: {self.performance['candles_received']} "
                        f"({candles_per_second:.2f}/s)\n"
                        f"- Batches processed: {self.performance['batches_processed']} "
                        f"({batches_per_second:.2f}/s)\n"
                        f"- Average batch processing time: {avg_processing_time:.4f}s\n"
                        f"- Max buffer size: {self.performance['buffer_high_water']} bytes\n"
                        f"- Empty receives: {self.performance['empty_receives']}\n"
                        f"- Reconnection attempts: {self.performance['reconnect_attempts']}\n"
                        f"- Subscribed symbols: {len(self.subscribed_symbols)}/"
                        f"{len(self.symbols)}"
                    )
                    
                    # Reset counters
                    self.performance['candles_received'] = 0
                    self.performance['batches_processed'] = 0
                    self.performance['empty_receives'] = 0
                    self.performance['buffer_high_water'] = 0
                    self.performance['last_report_time'] = current_time
                    
                    # Check for missing data
                    self._check_missing_candles()
            except Exception as e:
                self.logger_error(f"Error in performance monitor: {e}")
    
    def _is_market_closure_time(self):
        """Check if current time is during market closure hours (22:00-00:00)"""
        now = datetime.now(pytz.timezone('Europe/Warsaw'))
        hour = now.hour
        return 22 <= hour < 24  # Between 22:00 and 00:00
        
    def _check_missing_candles(self):
        """Check for missing candles for any symbol"""
        current_time = time.time() * 1000
        
        # Skip warning logs during market closure time
        if self._is_market_closure_time():
            # Send market closure notification, but not more than once per 60 minutes
            now = time.time()
            if now - self.performance.get('last_market_closure_notification', 0) > 3600:
                self.logger_info("Hours 22:00-00:00 - markets closed, missing data is expected")
                self.performance['last_market_closure_notification'] = now
            return
        
        for symbol in self.symbols:
            last_time = self.last_candle_time.get(symbol, 0)
            time_diff_minutes = (current_time - last_time) / (60 * 1000)
            
            # Log information if no candle received for more than 5 minutes
            if symbol in self.subscribed_symbols and time_diff_minutes > 5:
                self.logger_warning(f"No data for {symbol} for {time_diff_minutes:.1f} minutes")
                # Allow error propagation to sacred_data_line instead of resubscribing
    
    def stream_candle(self):
        """Main method for receiving candle stream"""
        conn_key = 'candle_stream'
        cm = ConnectionManager()
        connection = cm.get_connection(conn_key)
        
        if not connection:
            connection = ConnectionFactory.create_connection(
                HOST, STREAMING_PORT, name=conn_key)
            cm.add_connection(conn_key, connection)
        
        sstream_sock = connection.sock
        mli = self.market_loom_instance
        mli.stream_socks['candle_stream'] = sstream_sock
        self.logger_info("Connection for candle_stream added to stream_socks")
        
        # Subscribe to channels
        self._subscribe_all_symbols(sstream_sock)
        
        # Variables for batch processing
        batch_candles = defaultdict(list)
        last_batch_time = time.time()
        buffer = bytearray()  # Binary buffer instead of string
        
        while not connection.should_close.is_set():
            try:
                ready_socks, _, _ = select.select([sstream_sock], [], [], 10)
                if not ready_socks:
                    # Check if we have data in buffer waiting too long
                    if batch_candles and time.time() - last_batch_time > self.batch_time_threshold:
                        self._process_batch(batch_candles, 'M1')
                        batch_candles = defaultdict(list)
                        last_batch_time = time.time()
                    continue
                
                # Receive data from socket
                raw_data = sstream_sock.recv(BUFFER_SIZE_STREAMING_COAMND)
                
                if not raw_data:
                    self.performance['empty_receives'] += 1
                    self.performance['last_empty_receive'] = time.time()
                    
                    # Check if it's market closure time (22:00-00:00)
                    if self._is_market_closure_time():
                        # Treat lack of data more leniently during market closure
                        if self.performance['empty_receives'] % 60 == 1:  # Log less frequently
                            self.logger_info(
                                "No data in candle stream - hours 22:00-00:00, markets likely closed")
                        continue  # Continue normally, don't generate error
                    
                    # During normal hours, notify about missing data
                    if (self.performance['empty_receives'] % 10 == 1 or 
                        time.time() - self.performance.get('last_empty_notification', 0) > 300):
                        self.logger_critical(
                            "No data received in candle stream - socket returned zero bytes")
                        self.telegram_send("🕯️ No data received in candle stream!")
                        self.performance['last_empty_notification'] = time.time()
                    
                    # Check if problem persists for a longer time (outside closure hour)
                    if self.performance['empty_receives'] > 20:
                        # Allow error to propagate by raising exception, which will be handled in sacred_data_line
                        raise socket.error("No data in candle stream for an extended period")
                
                # Update statistics
                mli.last_receive_time['candle_stream'] = time.time()
                
                # Append new data to buffer
                buffer.extend(raw_data)
                
                # Update buffer size statistics
                self.performance['buffer_high_water'] = max(
                    self.performance['buffer_high_water'], len(buffer))
                
                # Process complete JSON lines
                lines_processed = 0
                while b'\n\n' in buffer and lines_processed < 1000:
                    lines_processed += 1
                    
                    # Find end of line position
                    newline_pos = buffer.find(b'\n\n')
                    if newline_pos == -1:
                        break
                    
                    # Extract line from buffer
                    line_bytes = buffer[:newline_pos]
                    buffer = buffer[newline_pos + 1:]  # Remove processed line
                    
                    if not line_bytes:
                        continue
                    
                    try:
                        # Parse JSON only for needed line
                        line_str = line_bytes.decode('utf-8')
                        candle_data = orjson.loads(line_str)
                        
                        if isinstance(candle_data, dict) and candle_data.get("command") == "candle":
                            data_part = candle_data["data"]
                            symbol = data_part.get("symbol")
                            
                            if symbol not in self.symbols_set:
                                continue
                            
                            # Update last candle time for this symbol
                            self.last_candle_time[symbol] = data_part.get("ctm", 0)
                            
                            # Mark symbol as active
                            self.subscribed_symbols.add(symbol)
                            
                            # Convert candle data to unified format
                            candle_info = {
                                "ctm": data_part.get("ctm"),
                                "ctmString": data_part.get("ctmString", ""),
                                "open": data_part.get("open"),
                                "close": data_part.get("close"),
                                "high": data_part.get("high"),
                                "low": data_part.get("low"),
                                "vol": data_part.get("vol", 0.0),
                                "quoteId": data_part.get("quoteId", 0)
                            }
                            
                            # Add to current batch of candles
                            batch_candles[symbol].append(candle_info)
                            self.performance['candles_received'] += 1
                            
                            # Check if we've reached batch size thresholds
                            total_candles = sum(len(candles) for candles in batch_candles.values())
                            if (total_candles >= self.batch_size_threshold or 
                                time.time() - last_batch_time > self.batch_time_threshold):
                                self._process_batch(batch_candles, 'M1')
                                batch_candles = defaultdict(list)
                                last_batch_time = time.time()
                    
                    except UnicodeDecodeError as e:
                        self.logger_error(f"UTF-8 decoding error: {e}, skipped some data")
                        continue
                    except orjson.JSONDecodeError as e:
                        self.logger_error(f"JSON parsing error: {e}, line: {line_bytes[:100]}...")
                        continue
                
                # If buffer is very large, something is probably wrong
                if len(buffer) > self.stream_buffer_size * 0.9:
                    self.logger_warning(f"Buffer is almost full ({len(buffer)} bytes), clearing...")
                    buffer.clear()
                
                # Check if there's data in batch buffer waiting to be processed
                if batch_candles and time.time() - last_batch_time > self.batch_time_threshold:
                    self._process_batch(batch_candles, 'M1')
                    batch_candles = defaultdict(list)
                    last_batch_time = time.time()
                
            except (socket.error, Exception) as e:
                if connection.should_close.is_set():
                    self.logger_info("Connection set to close, ending candle receiving loop")
                    break
                
                # Check if it's market closure time (22:00-00:00)
                if self._is_market_closure_time() and isinstance(e, socket.error):
                    # Treat connection problems more leniently during market closure hours
                    self.logger_warning(f"Error in stream_candle during market closure hours (22:00-00:00): {e}")
                    # Wait longer before retrying during closure hours
                    time.sleep(80)  # Wait longer instead of immediately propagating error
                    continue  # Try again in loop instead of propagating error
                
                # During normal hours - standard error handling
                self.logger_error(f"Error in stream_candle: {e}")
                self.logger_error(traceback.format_exc())
                self.telegram_send(f"🚨 Error in candle stream: {e}")
                
                # Allow error to propagate up
                # This will cause sacred_data_line to handle the error by resetting connection
                raise
    
    def _process_batch(self, batch_candles, interval):
        """Process batch of candles with performance measurement"""
        start_time = time.time()
        try:
            # Actual batch processing
            self.received_candle_batch(batch_candles, interval)
            self.performance['batches_processed'] += 1
            
            # Measure processing time
            processing_time = time.time() - start_time
            self.performance['processing_times'].append(processing_time)
            
            # Adaptive batch size adjustment
            self._adapt_batch_parameters(processing_time)
        except Exception as e:
            self.logger_error(f"Error during candle batch processing: {e}")
            self.logger_error(traceback.format_exc())
    
    def _adapt_batch_parameters(self, processing_time):
        """Adjust batch parameters based on performance"""
        # If processing takes longer than 80% of time between batches
        if processing_time > self.batch_time_threshold * 0.8:
            # Decrease batch size
            self.batch_size_threshold = max(10, int(self.batch_size_threshold * 0.8))
            if processing_time > self.batch_time_threshold * 1.5:
                # Drastic reduction for very slow processing
                self.batch_size_threshold = max(5, int(self.batch_size_threshold * 0.5))
                self.logger_warning(
                    f"Drastic batch size reduction to {self.batch_size_threshold} "
                    f"(processing: {processing_time:.4f}s)")
        # If processing is very fast
        elif processing_time < self.batch_time_threshold * 0.3 and self.batch_size_threshold < 500:
            # Increase batch size
            self.batch_size_threshold = min(500, int(self.batch_size_threshold * 1.2))
    
    def _subscribe_all_symbols(self, sstream_sock):
        """Subscribe to all symbols"""
        with self.socket_lock:
            get_candles_command_base = {
                "command": "getCandles",
                "streamSessionId": self.session_id
            }
            
            for symbol in self.symbols:
                time.sleep(0.5)  # Delay between subscriptions
                get_candles_command = get_candles_command_base.copy()
                get_candles_command["symbol"] = symbol
                
                try:
                    sstream_sock.sendall(orjson.dumps(get_candles_command) + b'\n')
                    self.logger_info(f"Candle subscription for {symbol}")
                except Exception as e:
                    self.logger_error(f"Subscription error for {symbol}: {e}")
                    continue
            
            self.logger_info("Completed candle data subscription for all symbols")
    
    def received_candle_batch(self, batch_candles, interval):
        """Process received candles in batch"""
        mli = self.market_loom_instance

        for symbol, candles_list in batch_candles.items():
            if not candles_list or symbol not in self.symbols_set:
                continue

            # Update DataFrame and aggregate data
            mli.historical_data.update_dataframe(symbol, interval, candles_list)

            # Save to Azure Blob if enabled
            if SAVE_DATA_IN_BLOB:
                IO_THREAD_POOL.submit(mli.historical_data.save_to_azure_blob, symbol)

            # Aggregate candle data
            mli.historical_data.candle_data_aggregator.candle_data_aggregator(
                symbol,
                candles_list,
                mli.historical_data.update_dataframe,
                mli.historical_data.save_to_disk
            )
