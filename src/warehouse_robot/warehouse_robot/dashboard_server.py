import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import threading
import time
import json
import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# ─────────────────────────────────────────────
# SHARED STATE  (ROS thread writes, WS reads)
# ─────────────────────────────────────────────
state = {
    "robots":      {},   # robot_id -> {x,y,status,battery,task}
    "tasks":       {},   # task_id  -> {robot_id,goal_x,goal_y,deadline_in}
    "stats": {
        "total": 0, "met": 0, "missed": 0, "rate": 0.0
    },
    "grid_size":   30,
    "log":         []    # last 12 log lines
}
state_lock = threading.Lock()

# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app      = FastAPI()
clients  = set()

HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>Warehouse Robot Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d1117;
    color: #e6edf3;
    font-family: 'Courier New', monospace;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 20px;
    gap: 16px;
  }
  h1 { font-size: 1.4rem; color: #58a6ff; letter-spacing: 2px; }

  /* ── top bar ── */
  #stats {
    display: flex;
    gap: 24px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px 24px;
    font-size: 0.85rem;
  }
  .stat-label { color: #8b949e; }
  .stat-value { color: #58a6ff; font-weight: bold; font-size: 1rem; }

  /* ── robot cards ── */
  #robot-cards {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    justify-content: center;
  }
  .robot-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px 16px;
    min-width: 160px;
    font-size: 0.78rem;
  }
  .robot-card .name  { font-size: 0.9rem; font-weight: bold; margin-bottom: 4px; }
  .robot-card .pos   { color: #8b949e; }
  .battery-bar {
    height: 6px;
    border-radius: 3px;
    margin-top: 6px;
    background: #21262d;
    overflow: hidden;
  }
  .battery-fill { height: 100%; transition: width 0.4s; border-radius: 3px; }

  /* ── grid ── */
  #grid-wrap {
    position: relative;
    border: 1px solid #30363d;
    border-radius: 4px;
    overflow: hidden;
  }
  canvas { display: block; }

  /* ── log ── */
  #log {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px 14px;
    width: 100%;
    max-width: 920px;
    font-size: 0.72rem;
    color: #8b949e;
    height: 140px;
    overflow-y: auto;
  }
  #log .warn  { color: #e3b341; }
  #log .error { color: #f85149; }
  #log .info  { color: #3fb950; }

  /* status colours */
  .status-IDLE     { color: #3fb950; }
  .status-ACTIVE   { color: #58a6ff; }
  .status-CRASHED  { color: #f85149; }
  .status-CHARGING { color: #e3b341; }
</style>
</head>
<body>
<h1>⬡ WAREHOUSE ROBOT DASHBOARD</h1>

<div id="stats">
  <div><div class="stat-label">TASKS TOTAL</div><div class="stat-value" id="s-total">0</div></div>
  <div><div class="stat-label">DEADLINE MET</div><div class="stat-value" id="s-met">0</div></div>
  <div><div class="stat-label">DEADLINE MISSED</div><div class="stat-value" id="s-missed">0</div></div>
  <div><div class="stat-label">SUCCESS RATE</div><div class="stat-value" id="s-rate">0%</div></div>
</div>

<div id="robot-cards"></div>

<div id="grid-wrap">
  <canvas id="grid" width="600" height="600"></canvas>
</div>

<div id="log"><em>Waiting for robots...</em></div>

<script>
const CELL  = 20;
const GRID  = 30;
const canvas = document.getElementById('grid');
const ctx    = canvas.getContext('2d');

const ROBOT_COLORS = {
  robot_1: '#58a6ff',
  robot_2: '#3fb950',
  robot_3: '#e3b341',
  robot_4: '#ff7b72',
  robot_5: '#d2a8ff',
  robot_6: '#79c0ff',
};

function robotColor(id) {
  return ROBOT_COLORS[id] || '#e6edf3';
}

function batteryColor(pct) {
  if (pct > 50) return '#3fb950';
  if (pct > 25) return '#e3b341';
  return '#f85149';
}

let lastState = null;

function drawGrid(data) {
  // Background
  ctx.fillStyle = '#0d1117';
  ctx.fillRect(0, 0, CELL * GRID, CELL * GRID);

  // Grid lines
  ctx.strokeStyle = '#21262d';
  ctx.lineWidth   = 0.5;
  for (let i = 0; i <= GRID; i++) {
    ctx.beginPath();
    ctx.moveTo(i * CELL, 0);
    ctx.lineTo(i * CELL, CELL * GRID);
    ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(0, i * CELL);
    ctx.lineTo(CELL * GRID, i * CELL);
    ctx.stroke();
  }

  // Goal markers
  for (const [tid, task] of Object.entries(data.tasks)) {
    const gx = task.goal_x * CELL + CELL / 2;
    const gy = task.goal_y * CELL + CELL / 2;
    const col = robotColor(task.robot_id);
    ctx.strokeStyle = col;
    ctx.lineWidth   = 1.5;
    ctx.globalAlpha = 0.5;
    ctx.beginPath();
    ctx.moveTo(gx - 5, gy - 5); ctx.lineTo(gx + 5, gy + 5);
    ctx.moveTo(gx + 5, gy - 5); ctx.lineTo(gx - 5, gy + 5);
    ctx.stroke();
    ctx.globalAlpha = 1.0;
  }

  // Robots
  for (const [rid, robot] of Object.entries(data.robots)) {
    const px  = robot.x * CELL + CELL / 2;
    const py  = robot.y * CELL + CELL / 2;
    const col = robotColor(rid);
    const r   = 7;

    // Glow for crashed
    if (robot.status === 'CRASHED') {
      ctx.shadowColor = '#f85149';
      ctx.shadowBlur  = 12;
    } else if (robot.status === 'CHARGING') {
      ctx.shadowColor = '#e3b341';
      ctx.shadowBlur  = 8;
    } else {
      ctx.shadowBlur = 0;
    }

    ctx.fillStyle = col;
    ctx.beginPath();
    ctx.arc(px, py, r, 0, Math.PI * 2);
    ctx.fill();

    // Robot label
    ctx.shadowBlur  = 0;
    ctx.fillStyle   = '#0d1117';
    ctx.font        = 'bold 8px Courier New';
    ctx.textAlign   = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(rid.replace('robot_', 'R'), px, py);

    // Line to goal
    if (robot.task && data.tasks[robot.task]) {
      const t  = data.tasks[robot.task];
      const gx = t.goal_x * CELL + CELL / 2;
      const gy = t.goal_y * CELL + CELL / 2;
      ctx.strokeStyle = col;
      ctx.lineWidth   = 1;
      ctx.globalAlpha = 0.25;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(gx, gy);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.globalAlpha = 1.0;
    }
  }
  ctx.shadowBlur = 0;
}

function updateCards(data) {
  const container = document.getElementById('robot-cards');
  container.innerHTML = '';
  for (const [rid, robot] of Object.entries(data.robots)) {
    const col   = robotColor(rid);
    const bCol  = batteryColor(robot.battery);
    const task  = robot.task ? `Task ${robot.task}` : 'idle';
    container.innerHTML += `
      <div class="robot-card" style="border-color:${col}33">
        <div class="name" style="color:${col}">${rid}</div>
        <div class="pos">pos (${robot.x}, ${robot.y})</div>
        <div>status: <span class="status-${robot.status}">${robot.status}</span></div>
        <div>task: ${task}</div>
        <div>battery: <span style="color:${bCol}">${robot.battery.toFixed(0)}%</span></div>
        <div class="battery-bar">
          <div class="battery-fill" style="width:${robot.battery}%;background:${bCol}"></div>
        </div>
      </div>`;
  }
}

function updateStats(data) {
  document.getElementById('s-total').textContent  = data.stats.total;
  document.getElementById('s-met').textContent    = data.stats.met;
  document.getElementById('s-missed').textContent = data.stats.missed;
  document.getElementById('s-rate').textContent   = data.stats.rate.toFixed(1) + '%';
}

function updateLog(data) {
  const logEl = document.getElementById('log');
  if (!data.log || data.log.length === 0) return;
  logEl.innerHTML = data.log.slice(-12).map(line => {
    if (line.includes('WARN') || line.includes('LOW BATTERY') || line.includes('MISSED'))
      return `<div class="warn">${line}</div>`;
    if (line.includes('ERROR') || line.includes('CRASH') || line.includes('DEPLETED'))
      return `<div class="error">${line}</div>`;
    return `<div class="info">${line}</div>`;
  }).join('');
  logEl.scrollTop = logEl.scrollHeight;
}

// WebSocket connection with auto-reconnect
function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    lastState  = data;
    drawGrid(data);
    updateCards(data);
    updateStats(data);
    updateLog(data);
  };
  ws.onclose = () => setTimeout(connect, 1500);
  ws.onerror = () => ws.close();
}

connect();
</script>
</body>
</html>
"""


@app.get("/")
async def index():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await asyncio.sleep(0.3)
            with state_lock:
                payload = json.dumps(state)
            await ws.send_text(payload)
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


# ─────────────────────────────────────────────
# ROS2 NODE
# ─────────────────────────────────────────────
class DashboardNode(Node):

    def __init__(self):
        super().__init__('dashboard_node')

        self.create_subscription(String, 'robot_heartbeat',  self.heartbeat_cb,   10)
        self.create_subscription(String, 'task_completion',  self.completion_cb,  10)
        self.create_subscription(String, 'robot_registration', self.register_cb,  10)

        # Subscribe to allocator log topic we'll add below
        self.create_subscription(String, 'dashboard_log',   self.log_cb,         10)

        # Also subscribe to task assignments via a new broadcast topic
        self.create_subscription(String, 'task_assignment', self.assignment_cb,   10)

        self.get_logger().info("Dashboard Node started — open http://localhost:8000")

    def register_cb(self, msg):
        parts = msg.data.split(",")
        if len(parts) < 3:
            return
        robot_id = parts[0]
        with state_lock:
            if robot_id not in state["robots"]:
                state["robots"][robot_id] = {
                    "x": int(parts[1]), "y": int(parts[2]),
                    "status": "IDLE", "battery": 100.0, "task": None
                }

    def heartbeat_cb(self, msg):
        parts = msg.data.split(",")
        if len(parts) < 4:
            return
        robot_id = parts[0]
        with state_lock:
            if robot_id not in state["robots"]:
                state["robots"][robot_id] = {
                    "x": 0, "y": 0,
                    "status": "IDLE", "battery": 100.0, "task": None
                }
            state["robots"][robot_id]["x"]       = int(parts[1])
            state["robots"][robot_id]["y"]       = int(parts[2])
            state["robots"][robot_id]["status"]  = parts[3]
            if len(parts) > 4:
                state["robots"][robot_id]["battery"] = float(parts[4])

    def completion_cb(self, msg):
        parts = msg.data.split(",")
        if len(parts) < 2:
            return
        robot_id = parts[0]
        task_id  = parts[1]
        with state_lock:
            if robot_id in state["robots"]:
                state["robots"][robot_id]["task"]   = None
                state["robots"][robot_id]["status"] = "IDLE"
            state["tasks"].pop(task_id, None)

    def assignment_cb(self, msg):
        # Format: task_id,robot_id,goal_x,goal_y,deadline_in
        parts = msg.data.split(",")
        if len(parts) < 4:
            return
        task_id  = parts[0]
        robot_id = parts[1]
        goal_x   = int(parts[2])
        goal_y   = int(parts[3])
        deadline = float(parts[4]) if len(parts) > 4 else 0.0
        with state_lock:
            state["tasks"][task_id] = {
                "robot_id":   robot_id,
                "goal_x":     goal_x,
                "goal_y":     goal_y,
                "deadline_in": deadline
            }
            if robot_id in state["robots"]:
                state["robots"][robot_id]["task"] = task_id

    def log_cb(self, msg):
        with state_lock:
            state["log"].append(msg.data)
            if len(state["log"]) > 40:
                state["log"] = state["log"][-40:]

    def update_stats(self, total, met, missed):
        with state_lock:
            state["stats"]["total"]  = total
            state["stats"]["met"]    = met
            state["stats"]["missed"] = missed
            state["stats"]["rate"]   = (met / total * 100.0) if total > 0 else 0.0


def run_uvicorn():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


def main(args=None):
    rclpy.init(args=args)
    node = DashboardNode()

    # Run FastAPI in background thread
    server_thread = threading.Thread(target=run_uvicorn, daemon=True)
    server_thread.start()

    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
