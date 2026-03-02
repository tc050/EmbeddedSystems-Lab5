import socket
import time
import csv
import random
import heapq

PI_LISTEN_IP = "0.0.0.0"
PI_LISTEN_PORT = 5005

# Controller gains
Kp = 0.15
Ki = 0.05

# --- Optional scheduling experiment knobs ---
INJECT_LOAD = False
LOAD_MAX_MS = 6

integral = 0.0
last_wall = None

# ---------- FIXED PRIORITY READY HEAP ----------
ready_heap = []
seq_counter = 0
max_queue_len = 0

class Task:
    def __init__(self, msg, addr, recv_wall, q_len_arrival):
        self.msg = msg
        self.addr = addr
        self.recv_wall = recv_wall
        self.q_len_arrival = q_len_arrival

# ---------- UDP SETUP ----------
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((PI_LISTEN_IP, PI_LISTEN_PORT))
sock.settimeout(1.0)

# ---------- LOGGING ----------
log_file = open("pi_rt_log.csv", "w", newline="")
writer = csv.writer(log_file)
writer.writerow([
    "recv_wall_time", "sender_ip", "sender_port",
    "msg_type", "seq", "t_sim", "temp", "setpoint", "dt_sim",
    "dt_wall", "err", "integral", "u_cmd",
    "compute_ms", "send_wall_time", "loop_latency_ms",
    "injected_load_ms", "late_flag",
    "queue_len_on_arrival", "queue_len_on_start", "max_queue_len_seen",
    "priority_used"
])

print(f"[Pi] Listening on UDP {PI_LISTEN_IP}:{PI_LISTEN_PORT}")

try:
    while True:
        try:
            # ---------- RECEIVE ----------
            data, addr = sock.recvfrom(4096)
            recv_wall = time.time()
            msg = data.decode("utf-8", errors="ignore").strip()

            # ---- Handshake ----
            if msg.startswith("HELLO"):
                parts = msg.split(",")
                matlab_port = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else addr[1]
                sock.sendto(b"ACK\n", (addr[0], matlab_port))
                print(f"[Pi] ACK -> {(addr[0], matlab_port)}")
                continue

            # ---- Parse DATA ----
            parts = msg.split(",")
            if len(parts) >= 6 and parts[0] == "DATA":
                dt_sim = float(parts[5])

                # Priority definition
                priority = dt_sim   # smaller period = higher priority

                q_len_arrival = len(ready_heap)
                task = Task(msg, addr, recv_wall, q_len_arrival)

                heapq.heappush(ready_heap, (priority, seq_counter, task))
                seq_counter += 1

                max_queue_len = max(max_queue_len, len(ready_heap))

            # ---------- PRIORITY EXECUTION ----------
            while ready_heap:
                q_len_start = len(ready_heap)
                priority, _, task = heapq.heappop(ready_heap)

                compute_start = time.time()

                parts = task.msg.split(",")

                seq = int(parts[1])
                t_sim = float(parts[2])
                temp = float(parts[3])
                sp = float(parts[4])
                dt_sim = float(parts[5])

                # ---- Wall-clock dt ----
                now = time.time()
                if last_wall is None:
                    dt_wall = dt_sim
                else:
                    dt_wall = max(1e-6, now - last_wall)
                last_wall = now

                # ---- Optional load injection ----
                injected = 0.0
                if INJECT_LOAD:
                    injected = random.uniform(0, LOAD_MAX_MS)
                    end = time.time() + injected / 1000.0
                    while time.time() < end:
                        pass

                # ---- PI control ----
                err = sp - temp
                integral += err * dt_wall
                u_cmd = Kp * err + Ki * integral
                u_cmd = max(0.0, min(1.0, u_cmd))

                compute_end = time.time()
                compute_ms = (compute_end - compute_start) * 1000.0

                # ---- Send command ----
                matlab_port = 5006
                out = f"CMD,{seq},{t_sim:.6f},{u_cmd:.4f},{compute_ms:.3f}\n".encode("utf-8")

                send_wall = time.time()
                sock.sendto(out, (task.addr[0], matlab_port))

                loop_latency_ms = (send_wall - task.recv_wall) * 1000.0
                late = 1 if compute_ms > (dt_sim * 1000.0) else 0

                writer.writerow([
                    task.recv_wall, task.addr[0], task.addr[1],
                    "DATA", seq, t_sim, temp, sp, dt_sim,
                    dt_wall, err, integral, u_cmd,
                    compute_ms, send_wall, loop_latency_ms,
                    injected, late,
                    task.q_len_arrival, q_len_start, max_queue_len,
                    priority
                ])
                log_file.flush()

        except socket.timeout:
            continue

except KeyboardInterrupt:
    pass

finally:
    log_file.close()
    sock.close()
    print("[Pi] Saved pi_rt_log.csv and closed socket")