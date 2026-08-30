#!/bin/bash
set -e

echo "========================================="
echo "🥛 The Milk-Run Fleet Tracker - Run All 🚐"
echo "========================================="

echo "🧹 Cleaning up any existing stray processes..."
pkill -f "spring-boot:run" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true
pkill -f "tsx src/index.ts" 2>/dev/null || true

# 1. Start Infrastructure
echo "➡️  Starting Infrastructure (Docker)..."
docker compose up -d

# 2. Start Backend
echo "➡️  Starting Backend (Spring Boot)..."
cd backend
mvn spring-boot:run -P local-dev > /tmp/milkrun-backend.log 2>&1 &
BACKEND_PID=$!
cd ..

# 3. Start Frontend
echo "➡️  Starting Frontend (Vite)..."
cd frontend
npm install --no-fund --no-audit
npm run dev > /tmp/milkrun-frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..

# 4. Start Simulator
echo "➡️  Starting Simulator..."
cd simulator
npm install --no-fund --no-audit
npm start > /tmp/milkrun-simulator.log 2>&1 &
SIMULATOR_PID=$!
cd ..

echo ""
echo "========================================="
echo "✅ All services started successfully!   "
echo "========================================="
echo "📊 Dashboard (Frontend):  http://localhost:5173"
echo "📡 API (Backend):         http://localhost:8080"
echo "📈 Metrics (Grafana):     http://localhost:3001"
echo "========================================="
echo ""
echo "If you want to view logs in another terminal:"
echo " 🔹 tail -f /tmp/milkrun-backend.log"
echo " 🔹 tail -f /tmp/milkrun-frontend.log"
echo " 🔹 tail -f /tmp/milkrun-simulator.log"
echo ""
echo "🛑 Press [Ctrl+C] to stop all services and tear down infrastructure."

# Cleanup on exit
function cleanup() {
    echo ""
    echo "🛑 Stopping Java, Node, and Vite services..."
    kill $BACKEND_PID $FRONTEND_PID $SIMULATOR_PID 2>/dev/null || true
    echo "🛑 Stopping Infrastructure (Docker Compose)..."
    docker compose down
    echo "👋 Goodbye!"
    exit
}

trap cleanup INT TERM

# Wait to keep the script running and hold the terminal
wait
