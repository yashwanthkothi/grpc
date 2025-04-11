import grpc
import trade_operation_pb2
import trade_operation_pb2_grpc
import psutil
import os, time
import socket
import subprocess
from typing import Optional, Dict, Any, Tuple, List

class SystemDataFetcher:
    def __init__(self):
        self.metrics = {
            'server_name': self._get_hostname(),
            'cpu_temp': None,
            'motherboard_temp': None,
            'exanic_temp': None,
            'hdd_usage': None,
            'ram_usage': None,
            'clock_drift': None,
            'ptp_sync_status': None,
            'page_faults': None,
            'fan_failure': None
        }

    def fetch_all_metrics(self) -> Dict[str, Any]:
        self.metrics.update({
            'cpu_temp': self._get_cpu_temperature(),
            'motherboard_temp': self._get_motherboard_temperature(),
            'exanic_temp': self._get_exanic_temperature(),
            'hdd_usage': self._get_hdd_space_usage(),
            'ram_usage': self._get_ram_usage(),
            'clock_drift': self._get_clock_drift(),
            'ptp_sync_status': self._get_ptp_sync_status(),
            'page_faults': self._get_page_faults(),
            'fan_failure': not self._check_fan_status()  # Inverted for fan_failure
        })
        return self.metrics

    def _get_hostname(self) -> str:
        """Get the system hostname using subprocess with fallbacks"""
        try:
            # Primary method: Use subprocess to run `hostname` command
            result = subprocess.run(["hostname"], capture_output=True, text=True, timeout=2)
            hostname = result.stdout.strip()
            if hostname and hostname != 'localhost':
                return hostname

            # Fallback to socket.gethostname()
            hostname = socket.gethostname()
            if hostname and hostname != 'localhost':
                return hostname

            # Final fallback to environment variable
            return os.environ.get('HOSTNAME', 'unknown-server')
        except Exception:
            return 'unknown-server'

    def _get_cpu_temperature(self) -> Optional[int]:
        try:
            for zone in os.listdir("/sys/class/thermal"):
                if zone.startswith("thermal_zone"):
                    with open(f"/sys/class/thermal/{zone}/temp", "r") as f:
                        temp = int(f.read().strip()) / 1000
                        if temp > 0:
                            return int(round(temp))
                        print(f'cpu temp : {temp}')
            return None
        except Exception:
            return None

    def _get_motherboard_temperature(self) -> Optional[int]:
        try:
            for hwmon in os.listdir("/sys/class/hwmon"):
                hwmon_path = os.path.join("/sys/class/hwmon", hwmon)
                if os.path.isdir(hwmon_path):
                    for file in os.listdir(hwmon_path):
                        if file.startswith("temp") and file.endswith("_input"):
                            with open(os.path.join(hwmon_path, file), "r") as f:
                                temp = int(f.read().strip()) / 1000
                                if temp > 0:
                                    return int(round(temp))
                                print(f'motherboard temp : {temp}')
            return None
        except Exception:
            return None

    def _get_exanic_temperature(self) -> Optional[int]:
        try:
            result = os.popen("exanic-config | grep Temperature").read()
            if result:
                print(f'Exanic temp : {result}')
                return int(round(float(result.split(":")[1].strip().split()[0])))
            return None
        except Exception:
            return None

    def _get_hdd_space_usage(self) -> Optional[float]:
        try:
            return round(psutil.disk_usage('/').percent, 1)
        except Exception:
            return None

    def _get_ram_usage(self) -> Optional[float]:
        try:
            return round(psutil.virtual_memory().percent, 1)
        except Exception:
            return None

    def _get_ptp_sync_status(self) -> bool:
        """Check PTP synchronization status using multiple methods"""
        try:
            # Method 1: Check ptp4l status directly
            result = subprocess.run(["ptp4l", "-i", "enp1s0d4", "-m", "-q", "--summary"],
                                capture_output=True, text=True, timeout=3)
            if "master offset" in result.stdout:
                offset_line = [l for l in result.stdout.split('\n') if "master offset" in l][0]
                offset = abs(float(offset_line.split()[2]))
                return offset < 1000  # Consider synced if offset < 1us
            
            # Method 2: Use pmc (PTP Management Client)
            # result = subprocess.run(["pmc", "-u", "-b", "0", "-i", "enp1s0d4", "GET CURRENT_DATA_SET"],
            #                     capture_output=True, text=True, timeout=3)
            # if "master_offset" in result.stdout:
            #     offset_line = [l for l in result.stdout.split('\n') if "master_offset" in l][0]
            #     offset = abs(float(offset_line.split()[-1]))
            #     return offset < 1000  # Consider synced if offset < 1us
            
            # # Method 3: Check system clock sync status
            # result = subprocess.run(["chronyc", "tracking"], capture_output=True, text=True, timeout=3)
            # if "Leap status" in result.stdout:
            #     sync_line = [l for l in result.stdout.split('\n') if "Leap status" in l][0]
            #     return "Normal" in sync_line
            
            return False
        except Exception as e:
            print(f"PTP status check error: {e}")
            return False

    def _get_clock_drift(self) -> Optional[float]:
        """Get clock drift in milliseconds using multiple methods"""
        try:
            # Method 1: Use phc2sys output
            result = subprocess.run(["phc2sys", "-s", "enp1s0d4", "-w", "-m", "-q", "--summary"],
                                capture_output=True, text=True, timeout=3)
            if "offset" in result.stdout:
                drift_line = [l for l in result.stdout.split('\n') if "offset" in l][0]
                return float(drift_line.split()[1]) * 1000  # Convert to milliseconds
            
            # Method 2: Use chronyc
            # result = subprocess.run(["chronyc", "tracking"], capture_output=True, text=True, timeout=3)
            # if "Last offset" in result.stdout:
            #     offset_line = [l for l in result.stdout.split('\n') if "Last offset" in l][0]
            #     return float(offset_line.split()[3]) * 1000  # Convert to milliseconds
            
            # # Method 3: Use ntpq
            # result = subprocess.run(["ntpq", "-p"], capture_output=True, text=True, timeout=3)
            # if "offset" in result.stdout:
            #     offset_line = [l for l in result.stdout.split('\n') if "*" in l][0]
            #     return float(offset_line.split()[8])  # Already in milliseconds
            
            return None
        except Exception as e:
            print(f"Clock drift check error: {e}")
            return None

    # def _get_ptp_sync_status(self) -> bool:
    #     try:
    #         result = subprocess.run(["pmc", "-u", "-b", "0", "GET CURRENT_DATA_SET"],
    #                                capture_output=True, text=True, timeout=3)
    #         if "master_offset" in result.stdout:
    #             lines = result.stdout.split('\n')
    #             for line in lines:
    #                 if "master_offset" in line:
    #                     offset = float(line.split()[-1])
    #                     return abs(offset) < 1000  # < 1us offset
    #         return False
    #     except Exception:
    #         return False

    # def _get_clock_drift(self) -> Optional[float]:
    #     try:
    #         result = subprocess.run(["phc_ctl", "sys", "cmp"], 
    #                               capture_output=True, text=True, timeout=3)
    #         if "offset" in result.stdout:
    #             return round(float(result.stdout.split("offset")[1].split()[0]), 3)
    #         return None
    #     except Exception:
    #         return None

    def _get_page_faults(self) -> int:
        try:
            with open("/proc/vmstat", "r") as f:
                data = f.read()
                minor = int([x for x in data.split() if "pgfault" in x][0].split()[1])
                major = int([x for x in data.split() if "pgmajfault" in x][0].split()[1])
                return minor + major  # Return total page faults
        except Exception:
            return 0

    def _check_fan_status(self) -> bool:
        try:
            for hwmon in os.listdir("/sys/class/hwmon"):
                hwmon_path = os.path.join("/sys/class/hwmon", hwmon)
                if os.path.isdir(hwmon_path):
                    for fan in [f for f in os.listdir(hwmon_path) if f.startswith("fan")]:
                        with open(os.path.join(hwmon_path, fan, "input"), "r") as f:
                            if int(f.read().strip()) == 0:
                                return False
            return True
        except Exception:
            return True  # Assume OK if can't check

# Create an class NetworkDataFetcher to Handle the Networkinng Stuffs
class TelnetChecker:
    def __init__(self):
        self.targets = [
            {"ip": "172.19.13.85", "port": 10829},
            {"ip": "172.28.124.30", "port": 10990}  # Example second target
        ]
        self.timeout_seconds = 3  # Default timeout

    def check_telnet_connectivity(self) -> List[Dict[str, Any]]:
        results = []
        for target in self.targets:
            ip = target["ip"]
            port = target["port"]
            start_time = time.time()
            reachable = False
            error = ""
            
            try:
                # Using subprocess to execute telnet command
                command = f"telnet {ip} {port}"
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE
                )
                # print(process)
                
                # Wait for the process to complete or timeout
                try:
                    stdout, stderr = process.communicate(timeout=self.timeout_seconds)
                    output = stdout.decode('utf-8').strip()
                    
                    # Consider it successful if we see "Connected to" in output
                    # even if connection is closed immediately after
                    if "Connected to" in output:
                        reachable = True
                    elif process.returncode == 0:
                        reachable = True
                    else:
                        error = stderr.decode('utf-8').strip()
                        if not error and output:
                            error = output
                
                except subprocess.TimeoutExpired:
                    process.kill()
                    error = "Connection timed out"
                
                response_time = (time.time() - start_time) * 1000  # in milliseconds

            except Exception as e:
                error = str(e)
                response_time = 0
            
            results.append({
                "ip": ip,
                "port": port,
                "reachable": reachable,
                "error": error,
                "response_time": round(response_time, 3)
            })
            print(results)
        
        return results

class SystemDataProcessor:
    """Second class: Processes and validates data from Fetcher"""
    def __init__(self, data_fetcher: SystemDataFetcher, telnet_checker: TelnetChecker): 
        self.data_fetcher = data_fetcher
        self.telnet_checker = telnet_checker
        self.processed_data = None
        self.processed_telnet_data = None

    def process_data(self) -> Dict[str, Any]:
        """Process and validate the collected metrics"""
        raw_data = self.data_fetcher.fetch_all_metrics()
        
        # Convert and validate all fields according to proto requirements
        self.processed_data = {
            'server_name': raw_data['server_name'],
            'cpu_temp': int(round(raw_data['cpu_temp'])) if raw_data['cpu_temp'] is not None else 0,
            'motherboard_temp': int(round(raw_data['motherboard_temp'])) if raw_data['motherboard_temp'] is not None else 0,
            'exanic_temp': int(round(raw_data['exanic_temp'])) if raw_data['exanic_temp'] is not None else 0,
            'hdd_usage': round(raw_data['hdd_usage'], 1) if raw_data['hdd_usage'] is not None else 0.0,
            'ram_usage': round(raw_data['ram_usage'], 1) if raw_data['ram_usage'] is not None else 0.0,
            'clock_drift': round(raw_data['clock_drift'], 3) if raw_data['clock_drift'] is not None else 0.0,
            'ptp_sync_status': bool(raw_data['ptp_sync_status']),
            'page_faults': int(raw_data['page_faults']),
            'fan_failure': bool(raw_data['fan_failure'])
        }
        return self.processed_data

    def process_telnet_data(self) -> List[Dict[str, Any]]:
        """Process telnet connectivity data"""
        raw_telnet_data = self.telnet_checker.check_telnet_connectivity()
        self.processed_telnet_data = raw_telnet_data  # Already in the right format
        return self.processed_telnet_data

class SystemDataTransmitter:
    """Third class: Handles gRPC communication with server"""
    def __init__(self, server_address: str):
        self.server_address = server_address
        self.channel_options = [
            ('grpc.connect_timeout_ms', 5000),
            ('grpc.keepalive_time_ms', 10000),
        ]

    def _test_connection(self, host: str, port: int, timeout=3) -> bool:
        """Test TCP connection to server"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
            return True
        except Exception as e:
            print(f"Connection test failed to {host}:{port}: {e}")
            return False

    def send_data(self, data: Dict[str, Any]) -> bool:
        """Send processed data to gRPC server"""
        try:
            host, port = self.server_address.split(':')
            port = int(port)
        except ValueError:
            print(f"Invalid server address format: {self.server_address}")
            return False

        if not self._test_connection(host, port):
            print("Cannot establish connection to server")
            return False

        try:
            with grpc.insecure_channel(self.server_address, options=self.channel_options) as channel:
                stub = trade_operation_pb2_grpc.TradeOperationStub(channel)
                response = stub.SendSystemInfo(
                    trade_operation_pb2.SystemInfoRequest(
                        server_name=data['server_name'],
                        cpu_temp=data['cpu_temp'],
                        motherboard_temp=data['motherboard_temp'],
                        exanic_temp=data['exanic_temp'],
                        hdd_usage=data['hdd_usage'],
                        ram_usage=data['ram_usage'],
                        clock_drift=data['clock_drift'],
                        ptp_sync_status=data['ptp_sync_status'],
                        page_faults=data['page_faults'],
                        fan_failure=data['fan_failure']
                    )
                )
                print(f"Server response: {response.message}")
                return True
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()}: {e.details()}")
            return False
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return False
    
    def send_telnet_data(self, source_server: str, telnet_data: List[Dict[str, Any]]) -> bool:
        """Send telnet connectivity data to gRPC server"""
        try:
            host, port = self.server_address.split(':')
            port = int(port)
        except ValueError:
            print(f"Invalid server address format: {self.server_address}")
            return False

        if not self._test_connection(host, port):
            print("Cannot establish connection to server")
            return False

        try:
            with grpc.insecure_channel(self.server_address, options=self.channel_options) as channel:
                stub = trade_operation_pb2_grpc.TradeOperationStub(channel)
                
                # Prepare the request
                request = trade_operation_pb2.TelnetRequest(
                    source_server=source_server,
                    timeout_seconds=3  # Same timeout as used in TelnetChecker
                )
                
                # Add all targets to the request
                for target in telnet_data:
                    telnet_target = trade_operation_pb2.TelnetTarget(
                        ip=target['ip'],
                        port=target['port'],
                        reachable=target['reachable'],
                        error=target['error'],
                        response_time=target['response_time']
                    )
                    request.targets.append(telnet_target)
                
                # Send the request
                response = stub.CheckTelnet(request)
                
                # Print results (optional)
                for result in response.results:
                    status = "reachable" if result.reachable else "unreachable"
                    print(f"Telnet {result.ip}:{result.port} is {status} (response time: {result.response_time} ms)")
                
                return True
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()}: {e.details()}")
            return False
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return False

if __name__ == "__main__":
    SERVER_ADDRESS = "172.31.3.161:50051"
    #SERVER_ADDRESS = "65.0.171.207:50051"
    # Initialize all components
    fetcher = SystemDataFetcher()
    telnet_checker = TelnetChecker()
    processor = SystemDataProcessor(fetcher, telnet_checker)
    transmitter = SystemDataTransmitter(SERVER_ADDRESS)
    
    # Data flow: Fetcher → Processor → Transmitter
    print("Starting system monitoring...")
    processed_data = processor.process_data()
    print("Processed metrics:", processed_data)
    
    if transmitter.send_data(processed_data):
        print("Data successfully transmitted to server")
    else:
        print("Data transmission failed")

    # Process and send telnet connectivity data
    processed_telnet_data = processor.process_telnet_data()
    print("Telnet connectivity results:", processed_telnet_data)
    
    if transmitter.send_telnet_data(processed_data['server_name'], processed_telnet_data):
        print("Telnet data successfully transmitted to server")
    else:
        print("Telnet data transmission failed")

