import grpc
import json
import re
from concurrent import futures
import psycopg2
import subprocess
from psycopg2 import sql
import trade_operation_pb2
import trade_operation_pb2_grpc
# -------------------- Load Config --------------------
def load_config(path='config.json'):
    with open(path, 'r') as f:
        return json.load(f)
# -------------------- Database Class --------------------
class Database:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None
        self.created_tables = set()
        self._ensure_database_exists()
        self._connect_to_main_db()
    def _connect(self, dbname):
        return psycopg2.connect(
            host=self.config["DB_HOST"],
            port=self.config["DB_PORT"],
            dbname=dbname,
            user=self.config["DB_USER"],
            password=self.config["DB_PASSWORD"]
        )
    def _ensure_database_exists(self):
        conn = self._connect("postgres")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.config["DB_NAME"],))
        if not cur.fetchone():
            print(f"[INFO] Database '{self.config['DB_NAME']}' not found. Creating...")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.config["DB_NAME"])))
            print("[SUCCESS] Database created.")
        cur.close()
        conn.close()
    def _connect_to_main_db(self):
        self.conn = self._connect(self.config["DB_NAME"])
        self.cursor = self.conn.cursor()
    def _sanitize_table_name(self, server_name):
        """Sanitize the server_name to be a valid SQL table name."""
        return re.sub(r'\W+', '_', server_name.lower())
    def _ensure_table_exists(self, table_name):
        """Create the table if it doesn't exist."""
        if table_name in self.created_tables:
            return
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                server_name TEXT,
                cpu_temp INTEGER,
                motherboard_temp INTEGER,
                exanic_temp INTEGER,
                hdd_usage FLOAT,
                ram_usage FLOAT,
                clock_drift FLOAT,
                ptp_sync_status BOOLEAN,
                page_faults BIGINT,
                fan_failure BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(sql.Identifier(table_name))
        self.cursor.execute(query)
        self.conn.commit()
        self.created_tables.add(table_name)
    def insert_system_info(self, server_name, data):
        """Insert system metrics into a server-specific table."""
        table_name = self._sanitize_table_name(server_name)
        self._ensure_table_exists(table_name)
        insert_query = sql.SQL("""
            INSERT INTO {} (
                server_name, cpu_temp, motherboard_temp, exanic_temp,
                hdd_usage, ram_usage, clock_drift,
                ptp_sync_status, page_faults, fan_failure
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(table_name))
        self.cursor.execute(insert_query, data)
        self.conn.commit()
# -------------------- gRPC Service --------------------
class TradeOperationServicer(trade_operation_pb2_grpc.TradeOperationServicer):
    def __init__(self, db):
        self.db = db
    def SendSystemInfo(self, request, context):
        print(f"[INFO] Received System Info from {request.server_name}")
        try:
            data = (
                request.server_name,
                request.cpu_temp,
                request.motherboard_temp,
                request.exanic_temp,
                request.hdd_usage,
                request.ram_usage,
                request.clock_drift,
                request.ptp_sync_status,
                request.page_faults,
                request.fan_failure
            )
            self.db.insert_system_info(request.server_name, data)
            print(f"[SUCCESS] Data stored for server '{request.server_name}'")
            return trade_operation_pb2.systemInfoResponse(
                message=f"System info for '{request.server_name}' stored successfully."
            )
        except Exception as e:
            print(f"[ERROR] Failed to insert data: {e}")
            return trade_operation_pb2.systemInfoResponse(
                message=f"Failed to store system info for '{request.server_name}'."
            )
class ConnectivityService(trade_operation_pb2_grpc.ConnectivityServiceServicer):
    def CheckServerHealth(self, request, context):
        return trade_operation_pb2.NetworkInfoResponse(
            message="Server is healthy",
            is_healthy=True
        )
class TelnetService(trade_operation_pb2_grpc.TelnetServiceServicer):
    def ExecuteCommand(self, request, context):
        try:
            result = subprocess.run(request.command, shell=True, capture_output=True, text=True)
            return trade_operation_pb2.CommandResponse(result=result.stdout)
        except Exception as e:
            return trade_operation_pb2.CommandResponse(result=str(e))
# -------------------- gRPC Server Entry --------------------
def serve():
    config = load_config()
    db = Database(config)
    # Create the gRPC server and add all services
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    # Add all the services to the gRPC server
    trade_operation_pb2_grpc.add_TradeOperationServicer_to_server(TradeOperationServicer(db), server)
    trade_operation_pb2_grpc.add_ConnectivityServiceServicer_to_server(ConnectivityService(), server)
    trade_operation_pb2_grpc.add_TelnetServiceServicer_to_server(TelnetService(), server)
    # Start the server
    server.add_insecure_port('[::]:50051')
    print("[STARTED] gRPC Server is listening on port 50051")
    server.start()
    server.wait_for_termination()
if __name__ == "__main__":
    serve()
