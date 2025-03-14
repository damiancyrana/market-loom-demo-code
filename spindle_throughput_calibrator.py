class SpindleThroughputCalibrator:
    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self.queue = queue.Queue()
        self.last_send_time_lock = threading.Lock()
        self.last_send_time = 0
        self.thread = threading.Thread(target=self._process_queue, daemon=True)
        self.thread.start()

        self.chunked_queue = queue.Queue()
        self.last_send_time_chunked_lock = threading.Lock()
        self.last_send_time_chunked = 0
        self.chunked_thread = threading.Thread(
            target=self._process_chunked_queue, daemon=True)
        self.chunked_thread.start()

    @classmethod
    def get_instance(cls):
        """Singleton pattern implementation"""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = SpindleThroughputCalibrator()
        return cls._instance

    def send_command(self, sock_or_connection, command, timeout=TIMEOUT):
        """Send command to socket with rate limiting and wait for response"""
        event = threading.Event()
        response_container = []
        self.queue.put((sock_or_connection, command, event, response_container, timeout))
        event.wait()
        return response_container[0] if response_container else None

    def send_chunked_command_queued(self, sock_or_connection, command):
        """Send chunked command to socket with rate limiting and wait for response"""
        event = threading.Event()
        response_container = []
        self.chunked_queue.put((sock_or_connection, command, event, response_container))
        event.wait()
        return response_container[0] if response_container else None

    def _process_queue(self):
        """Process command queue with rate limiting"""
        while True:
            try:
                item = self.queue.get()
                sock_or_connection, command, event, response_container, timeout = item
                
                # Use lock for safe access to last_send_time
                with self.last_send_time_lock:
                    now = time.time()
                    elapsed = now - self.last_send_time
                    if elapsed < RATE_LIMIT_INTERVAL:
                        time.sleep(RATE_LIMIT_INTERVAL - elapsed)
                
                response = self._send_command_internal(
                    sock_or_connection, command, timeout)
                
                # Update last_send_time safely
                with self.last_send_time_lock:
                    self.last_send_time = time.time()
                
                response_container.append(response)
                event.set()
            except Exception as e:
                logger.error(f"Error in SpindleThroughputCalibrator _process_queue: {e}")
                event.set()

    def _process_chunked_queue(self):
        """Process chunked command queue with rate limiting"""
        while True:
            try:
                item = self.chunked_queue.get()
                sock_or_connection, command, event, response_container = item
                
                # Use lock for safe access to last_send_time_chunked
                with self.last_send_time_chunked_lock:
                    now = time.time()
                    elapsed = now - self.last_send_time_chunked
                    if elapsed < RATE_LIMIT_INTERVAL:
                        time.sleep(RATE_LIMIT_INTERVAL - elapsed)
                
                response = self._send_chunked_command_internal(
                    sock_or_connection, command)
                
                # Update last_send_time_chunked safely
                with self.last_send_time_chunked_lock:
                    self.last_send_time_chunked = time.time()
                
                response_container.append(response)
                event.set()
            except Exception as e:
                logger.error(f"Error in SpindleThroughputCalibrator _process_chunked_queue: {e}")
                event.set()

    def _send_command_internal(self, sock_or_connection, command, timeout):
        """Send command to socket and receive response"""
        try:
            if hasattr(sock_or_connection, 'sock'):
                sock = sock_or_connection.sock
            else:
                sock = sock_or_connection

            def convert_numpy_values(obj):
                """Convert numpy values to Python native types"""
                if isinstance(obj, dict):
                    return {k: convert_numpy_values(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numpy_values(elem) for elem in obj]
                elif isinstance(obj, np.generic):
                    value = obj.item()
                    return 0.0 if np.isnan(value) else value
                elif isinstance(obj, float) and np.isnan(obj):
                    return 0.0
                else:
                    return obj

            converted_command = convert_numpy_values(command)
            message = orjson.dumps(converted_command) + b"\n"
            sock.sendall(message)

            sock.settimeout(timeout)
            response_data = b""
            while True:
                try:
                    data = sock.recv(BUFFER_SIZE_COMMAND)
                    if data:
                        response_data += data
                        if response_data.endswith(b'\n'):
                            response_str = response_data.decode('utf-8').rstrip('\n')
                            response = orjson.loads(response_str)
                            if not handle_response_errors(response):
                                return None
                            return response
                    else:
                        break
                except socket.timeout:
                    logger.warning("Timeout exceeded while receiving response")
                    break
            logger.error("No complete response received or response is incomplete")
            return None
        except Exception as e:
            logger.error(f"Error in send_command: {e}")
            return None
        
    def _send_chunked_command_internal(self, sock_or_connection, command):
        """Send chunked command to socket and receive response"""
        try:
            if hasattr(sock_or_connection, 'sock'):
                sock = sock_or_connection.sock
            else:
                sock = sock_or_connection

            sock.sendall(orjson.dumps(command).decode('utf-8').encode('utf-8'))
            received_data = b""
            while True:
                part = sock.recv(BUFFER_SIZE_CHUNKED_COMMAND)
                received_data += part
                if b'\n' in part:
                    break

            if not received_data.strip():
                logger.info("No data received from server")
                return None
            try:
                response = orjson.loads(received_data)
                if not handle_response_errors(response):
                    return None
                return response
            except orjson.JSONDecodeError as e:
                logger.error(
                    f"JSON decoding error: {e}, Raw data received: "
                    f"{received_data.decode('utf-8')}")
                return None

        except Exception as e:
            logger.error(
                f"Error sending command {command} or receiving data: {e}")
            return None
