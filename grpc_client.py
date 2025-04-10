import grpc
import trade_operation_pb2
import trade_operation_pb2_grpc
import psutil
import os
import socket
import subprocess
from typing import Optional, Dict, Any, Tuple

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

            result = subprocess.run("exanic-config | grep Temperature", capture_output=True, text=True, shell=True)

            if result.stdout:

                print(f'Exanic temp : {result.stdout}')

                return int(round(float(result.stdout.split(":")[1].strip().split()[0])))

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

            result = subprocess.run(["ptp4l", "-i", "enp1s0d4", "-m", "-q", "--summary"],

                                    capture_output=True, text=True, timeout=3)

            if "master offset" in result.stdout:

                offset_line = [l for l in result.stdout.split('\n') if "master offset" in l][0]

                offset = abs(float(offset_line.split()[2]))

                return offset < 1000  # Consider synced if offset < 1us

            return False

        except Exception as e:

            print(f"PTP status check error: {e}")

            return False

    def _get_clock_drift(self) -> Optional[float]:

        """Get clock drift in milliseconds using multiple methods"""

        try:

            result = subprocess.run(["phc2sys", "-s", "enp1s0d4", "-w", "-m", "-q", "--summary"],

                                    capture_output=True, text=True, timeout=3)

            if "offset" in result.stdout:

                drift_line = [l for l in result.stdout.split('\n') if "offset" in l][0]

                return float(drift_line.split()[1]) * 1000  # Convert to milliseconds

            return None

        except Exception as e:

            print(f"Clock drift check error: {e}")

            return None

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

class SystemDataProcessor:

    """Second class: Processes and validates data from Fetcher"""

    def __init__(self, data_fetcher: SystemDataFetcher):

        self.data_fetcher = data_fetcher

        self.processed_data = None

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

class TelnetConnectivityChecker:

    def __init__(self, server_address: str):

        self.server_address = server_address

        self.channel = grpc.insecure_channel(server_address)

        self.stub = trade_operation_pb2_grpc.ConnectivityCheckerStub(self.channel)



    def check_connectivity(self, target_ips: list, port: int = 23, timeout_seconds: int = 5):

        request = trade_operation_pb2.TelnetRequest(

            target_ips=target_ips,

            port=port,

            timeout_seconds=timeout_seconds

        )

        try:

            response = self.stub.CheckTelnet(request)

            self._print_results(response.results)

        except grpc.RpcError as e:

            print(f"gRPC error occurred: {e.code()}: {e.details()}")



    def _print_results(self, results):

        for result in results:

            status = "REACHABLE" if result.reachable else "UNREACHABLE"

            print(f"{result.ip}: {status}")

            if not result.reachable:

                print(f"  Error: {result.error}")

if __name__ == "__main__":

    SERVER_ADDRESS = "192.168.0.118:50051"

    # Initialize all components

    fetcher = SystemDataFetcher()

    processor = SystemDataProcessor(fetcher)

    transmitter = SystemDataTransmitter(SERVER_ADDRESS)

    # Data flow: Fetcher → Processor → Transmitter

    print("Starting system monitoring...")

    processed_data = processor.process_data()

    print("Processed metrics:", processed_data)

    if transmitter.send_data(processed_data):

        print("Data successfully transmitted to server")

    else:

        print("Data transmission failed")

    checker = TelnetConnectivityChecker('192.168.0.118:50051')

    checker.check_connectivity(["192.168.0.119", "192.168.0.120"])






