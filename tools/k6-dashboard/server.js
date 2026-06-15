const express = require("express");
const { spawn } = require("child_process");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 5555;

// Path to k6 scripts — adjust if you move this folder
// Default assumes: <project-root>/tools/k6-dashboard/server.js
//                   <project-root>/tests/k6/performance_test.js
const K6_DIR = process.env.K6_DIR || path.resolve(__dirname, "../../tests/k6");
const K6_SCRIPT = "performance_test.js";
const BASE_URL = process.env.K6_TARGET_URL || "http://localhost:8000";

let currentProcess = null;
let currentScenario = null;

const SCENARIOS = {
    smoke: {
        label: "Smoke Test",
        profile: "smoke",
        duration: "1 menit · 5 VU",
    },
    load: {
        label: "Load Test",
        profile: "load",
        duration: "6 menit · ramp ke 500 VU",
    },
    stress: {
        label: "Stress Test",
        profile: "stress",
        duration: "13 menit · 1.000 → 3.000 VU",
    },
    spike: {
        label: "Spike Test",
        profile: "spike",
        duration: "~1m20s · lonjakan ke 1.500 VU",
    },
    soak: {
        label: "Soak Test",
        profile: "soak",
        duration: "~2 jam · 400 VU konstan",
    },
    load_slo: {
        label: "Load SLO (Sweet Spot)",
        profile: "load_slo",
        duration: "3 menit · VU custom",
        extraEnv: true,
    },
};

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

app.get("/scenarios", (req, res) => {
    const list = Object.entries(SCENARIOS).map(([id, s]) => ({
        id,
        label: s.label,
        profile: s.profile,
        duration: s.duration,
        extraEnv: !!s.extraEnv,
        running: currentScenario === id,
    }));
    res.json({ scenarios: list, active: currentScenario, baseUrl: BASE_URL });
});

// GET /run/:scenario?vu=200 — SSE stream of k6 output
app.get("/run/:scenario", (req, res) => {
    const scenarioId = req.params.scenario;
    const scenario = SCENARIOS[scenarioId];

    if (!scenario) {
        res.status(404).json({ error: "Scenario not found" });
        return;
    }

    if (currentProcess) {
        res.status(409).json({ error: "A test is already running" });
        return;
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.flushHeaders();

    const send = (type, data) => {
        res.write(`data: ${JSON.stringify({ type, data })}\n\n`);
    };

    send("status", `Memulai: ${scenario.label}`);

    const scriptPath = path.join(K6_DIR, K6_SCRIPT);
    const args = ["run", "-e", `BASE_URL=${BASE_URL}`, "-e", `TEST_PROFILE=${scenario.profile}`];

    if (scenario.extraEnv && req.query.vu) {
        args.push("-e", `VU_TARGET=${req.query.vu}`);
    }

    args.push(scriptPath);

    send("log", `$ k6 ${args.join(" ")}`);

    const proc = spawn("k6", args, {
        env: { ...process.env },
        shell: true, // helps resolve `k6` on Windows PATH
    });

    currentProcess = proc;
    currentScenario = scenarioId;

    proc.stdout.on("data", (data) => {
        data.toString().split("\n").filter(Boolean).forEach((line) => send("log", line));
    });

    proc.stderr.on("data", (data) => {
        data.toString().split("\n").filter(Boolean).forEach((line) => send("log", line));
    });

    proc.on("error", (err) => {
        send("error", `Gagal menjalankan k6: ${err.message}. Pastikan k6 sudah terinstall dan ada di PATH.`);
        currentProcess = null;
        currentScenario = null;
        res.end();
    });

    proc.on("close", (code) => {
        currentProcess = null;
        currentScenario = null;
        if (code === 0) {
            send("done", "Test selesai.");
        } else {
            send("error", `Proses berhenti dengan kode ${code}`);
        }
        res.end();
    });

    req.on("close", () => {
        if (currentProcess) {
            currentProcess.kill();
            currentProcess = null;
            currentScenario = null;
        }
    });
});

app.post("/stop", (req, res) => {
    if (!currentProcess) {
        res.status(404).json({ error: "No test running" });
        return;
    }
    currentProcess.kill("SIGTERM");
    currentProcess = null;
    currentScenario = null;
    res.json({ ok: true });
});

app.listen(PORT, () => {
    console.log(`\n  BankX K6 Performance Dashboard`);
    console.log(`  Buka di browser: http://localhost:${PORT}`);
    console.log(`  K6 scripts dir : ${K6_DIR}`);
    console.log(`  Target API     : ${BASE_URL}\n`);
});