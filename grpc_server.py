import grpc
from concurrent import futures
import json
import psycopg2
from psycopg2 import sql
import socket
import time
import re
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
        self.created_telnet_tables = set()
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
        return re.sub(r'\W+', '_', server_name.lower())

    def _ensure_table_exists(self, table_name):
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

    def _ensure_telnet_table_exists(self, table_name):
        if table_name in self.created_telnet_tables:
            return
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                source_server TEXT,
                target_ip TEXT,
                target_port INTEGER,
                reachable BOOLEAN,
                response_time FLOAT,
                error_message TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(sql.Identifier(table_name))
        self.cursor.execute(query)
        self.conn.commit()
        self.created_telnet_tables.add(table_name)

    def insert_telnet_data(self, source_server, telnet_data):
        table_name = "telnet_connectivity_results"
        self._ensure_telnet_table_exists(table_name)
        for result in telnet_data:
            insert_query = sql.SQL("""
                INSERT INTO {} (
                    source_server, target_ip, target_port,
                    reachable, response_time, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(table_name))
            self.cursor.execute(insert_query, (
                source_server,
                result['ip'],
                result['port'],
                result['reachable'],
                result['response_time'],
                result['error']
            ))
        self.conn.commit()

    def insert_system_info(self, server_name, data):
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


# -------------------- Unified gRPC Service --------------------
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
            return trade_operation_pb2.SystemInfoResponse(
                message=f"System info for '{request.server_name}' stored successfully."
            )
        except Exception as e:
            print(f"[ERROR] Failed to insert data: {e}")
            return trade_operation_pb2.SystemInfoResponse(
                message=f"Failed to store system info for '{request.server_name}'."
            )

    def CheckTelnet(self, request, context):
        print(f"[INFO] Received Telnet check request from {request.source_server}")
        try:
            telnet_data = []
            for target in request.targets:
                telnet_data.append({
                    'ip': target.ip,
                    'port': target.port,
                    'reachable': target.reachable,
                    'error': target.error,
                    'response_time': target.response_time
                })

            self.db.insert_telnet_data(request.source_server, telnet_data)

            response = trade_operation_pb2.TelnetResponse()
            for target in request.targets:
                result = response.results.add()
                result.ip = target.ip
                result.port = target.port
                result.reachable = target.reachable
                result.error = target.error
                result.response_time = target.response_time

            print(f"[SUCCESS] Telnet data stored for '{request.source_server}'")
            return response
        except Exception as e:
            print(f"[ERROR] Telnet data insert failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Telnet data processing failed")
            return trade_operation_pb2.TelnetResponse()


# -------------------- Server Entry Point --------------------
def serve():
    config = load_config()
    db = Database(config)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))

    trade_operation_pb2_grpc.add_TradeOperationServicer_to_server(TradeOperationServicer(db), server)

    server.add_insecure_port('[::]:50051')
    print("[STARTED] gRPC Server is listening on port 50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
