import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics to separate read/write observations
const transactionLatency = new Trend('tx_latency', true);
const balanceLatency = new Trend('balance_latency', true);
const errorRate = new Rate('errors');
const validReqCounter = new Counter('valid_requests');
const shieldedTxCounter = new Counter('shielded_tps');

function handleResult(success, res) {
    if (success) {
        validReqCounter.add(1);
        errorRate.add(false);
    } else if (res.status === 503 || res.status === 429) {
        shieldedTxCounter.add(1); // Ditangkis pertahanan (Bukan error aplikasi)
        errorRate.add(false);
    } else {
        errorRate.add(true); // Error murni (500, EOF, Timeout)
    }
}

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// =========================================================================
// DEFINISI PROFIL BEBAN (TEST PROFILES)
// =========================================================================

const profiles = {
    smoke: {
        smoke_scenario: {
            executor: 'constant-vus',
            vus: 5,
            duration: '1m',
        },
    },
    load: {
        load_scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '2m', target: 500 }, // Normal traffic ramp-up
                { duration: '3m', target: 500 }, // Steady state
                { duration: '1m', target: 0 },   // Scale down
            ],
        },
    },
    stress: {
        stress_scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '2m', target: 1000 },  // Quick ramp to high
                { duration: '5m', target: 1000 },  // Hold high load
                { duration: '2m', target: 2000 }, // Push to breaking point
                { duration: '2m', target: 3000 }, // Hold breaking point
                { duration: '2m', target: 0 },    // Scale down
            ],
        },
    },
    spike: {
        spike_scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '10s', target: 1500 }, // Sudden huge spike (flash sale)
                { duration: '1m', target: 1500 },  // Hold spike
                { duration: '10s', target: 0 },   // Instant drop
            ],
        },
    },
    soak: {
        soak_scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '5m', target: 400 }, // Ramp up
                { duration: '2h', target: 400 }, // Hold for hours (to check memory leak/resource exhaustion)
                { duration: '5m', target: 0 },   // Scale down
            ],
        },
    },
};

// Default profile: runs smoke, load, spike, and stress sequentially
const defaultProfiles = {
    smoke_scenario: {
        executor: 'constant-vus',
        vus: 5,
        duration: '30s',
    },
    load_scenario: {
        executor: 'ramping-vus',
        startTime: '35s', // Starts after smoke
        startVUs: 0,
        stages: [
            { duration: '1m', target: 500 },
            { duration: '3m', target: 500 },
            { duration: '30s', target: 0 },
        ],
    },
    spike_scenario: {
        executor: 'ramping-vus',
        startTime: '5m10s', // Starts after load
        startVUs: 0,
        stages: [
            { duration: '10s', target: 1500 },
            { duration: '1m', target: 1500 },
            { duration: '10s', target: 0 },
        ],
    },
    stress_scenario: {
        executor: 'ramping-vus',
        startTime: '6m40s', // Starts after spike
        startVUs: 0,
        stages: [
            { duration: '1m', target: 1000 },
            { duration: '2m', target: 2000 },
            { duration: '2m', target: 3000 },
            { duration: '1m', target: 0 },
        ],
    },
};

// =========================================================================
// K6 OPTIONS CONFIGURATION
// =========================================================================

const profileType = __ENV.TEST_PROFILE || 'default';
let activeScenarios = {};

if (profileType === 'default') {
    activeScenarios = defaultProfiles;
} else if (profiles[profileType]) {
    activeScenarios = profiles[profileType];
} else {
    console.error(`Invalid TEST_PROFILE: ${profileType}. Valid options are: default, smoke, load, stress, spike, soak`);
}

export const options = {
    scenarios: activeScenarios,
    thresholds: {
        // SLO Target: p95 latency < 500ms
        'http_req_duration': ['p(95)<500'],
        'tx_latency': ['p(95)<500'],
        'balance_latency': ['p(95)<300'], // Reads should ideally be faster
        // SLO Target: Error rate < 1%
        'errors': ['rate<0.01'],
    },
};

// =========================================================================
// HELPER LOGIC FOR REALISTIC DATA 
// =========================================================================

function generateHeaders(userId) {
    return {
        'Content-Type': 'application/json',
        'X-User-ID': userId.toString(),
        'Accept': 'application/json',
        // Simulate real browsers dropping realistic User-Agent headers
        'User-Agent': `k6-Performance-Test/1.0 (Real User Sim; VU: ${__VU})`,
    };
}

// =========================================================================
// VIRTUAL USER LIFESTYLES (SCENARIOS)
// =========================================================================

// Behavior 1: Read-Heavy User (Checks balance, maybe refreshes) - ~60% prevalence
function readHeavyUser(userId) {
    const params = { headers: generateHeaders(userId) };

    // Action: Check balance
    const res = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });

    balanceLatency.add(res.timings.duration);
    const success = check(res, {
        'is status 200': (r) => r.status === 200,
        'has balance data': (r) => r.body && r.body.includes('balance')
    });
    handleResult(success, res);

    // Realistic think time (reading screen)
    //sleep(randomIntBetween(0.1, 1));

    // Maybe check again (20% chance of impatient reload)
    if (Math.random() < 0.2) {
        const refreshRes = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
        balanceLatency.add(refreshRes.timings.duration);
        const refreshSuccess = check(refreshRes, { 'is status 200': (r) => r.status === 200 });
        handleResult(refreshSuccess, refreshRes);
        //sleep(randomIntBetween(1, 3));
    }
}

// Behavior 2: Active Transactor (Checks balance -> Transacts -> Checks status) - ~30% prevalence
function activeTransactor(userId) {
    const params = { headers: generateHeaders(userId) };

    // Action 1: Pre-check balance
    const balRes = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
    const balSuccess = check(balRes, { 'is status 200': (r) => r.status === 200 });
    handleResult(balSuccess, balRes);

    // Think about the transfer amount
    //sleep(0.5);

    // Action 2: Perform Transaction
    const type = Math.random() > 0.5 ? 'deposit' : 'withdraw';
    const amount = randomIntBetween(10, 5000); // 10 to 5000 units

    const payload = JSON.stringify({
        user_id: userId,
        amount: amount,
        type: type,
        description: `Realistic ${type} by user ${userId}`,
    });

    const txRes = http.post(`${BASE_URL}/transactions`, payload, params, { tags: { name: 'post-transaction' } });
    transactionLatency.add(txRes.timings.duration);

    const success = check(txRes, { 'post tx status is 202': (r) => r.status === 202 });
    handleResult(success, txRes);

    // User receives notification and clicks it
    //sleep(0.5);

    // Action 3: Check transaction status if creation was accepted
    if (success && txRes.json('id')) {
        const txId = txRes.json('id');
        const statusRes = http.get(http.url`${BASE_URL}/transactions/${txId}`, params, { tags: { name: 'get-transaction-status' } });
        const statusSuccess = check(statusRes, { 'is status 200 or 202': (r) => r.status === 200 || r.status === 202 });
        handleResult(statusSuccess, statusRes);
    }
}

// Behavior 3: API Client/Bot (High frequency, no think time) - ~10% prevalence
function apiClientBot(userId) {
    const params = { headers: generateHeaders(userId) };

    for (let i = 0; i < 10; i++) {
        // Poll balance rapidly
        const res = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
        balanceLatency.add(res.timings.duration);
        const botSuccess = check(res, { 'is status 200': (r) => r.status === 200 });
        handleResult(botSuccess, res);
        //sleep(0.5); // Minimal delay
    }
}

// =========================================================================
// MAIN FUNCTION (ROUTER)
// =========================================================================

export default function () {
    // Select a random user ID between 1 and 100,000 (realistic large user base)
    const randomUserId = randomIntBetween(1, 100000);

    // Probabilistic behavioral routing
    const randomBehavior = Math.random();

    if (randomBehavior < 0.6) {
        // 60% of traffic is just checking balance
        readHeavyUser(randomUserId);
    } else if (randomBehavior < 0.9) {
        // 30% of traffic makes transactions
        activeTransactor(randomUserId);
    } else {
        // 10% of traffic behaves like aggressive API polling
        apiClientBot(randomUserId);
    }
}
