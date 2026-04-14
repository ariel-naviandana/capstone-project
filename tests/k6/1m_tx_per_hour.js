import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
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
        shieldedTxCounter.add(1);
        errorRate.add(false);
    } else {
        errorRate.add(true);
    }
}

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// =========================================================================
// K6 OPTIONS CONFIGURATION (1 Million Tx / Hour)
// 1.000.000 / 3600 detik = ~278 request / detik
// =========================================================================

export const options = {
    scenarios: {
        one_million_per_hour: {
            // Gunakan constant-arrival-rate untuk menahan kecepatan K6 agar stabil, tidak beringas seperti sebelumnya
            executor: 'constant-arrival-rate',
            // Kecepatan di set ke 278 iterasi per detik
            rate: 278,
            timeUnit: '1s',
            // Durasi dijalankan 5 menit untuk mengecek stabilitas (bisa Anda ubah ke '1h' kalau mau test 1 jam beneran)
            duration: '5m',
            
            // Berapa jumlah thread awal yang disiapkan
            preAllocatedVUs: 50,
            // Opsional: izinkan naik VUs kalau server melambat, tapi tetap batas throughput di 278
            maxVUs: 1000,
        },
    },
    thresholds: {
        'http_req_duration': ['p(95)<500'],
        'tx_latency': ['p(95)<500'],
        'balance_latency': ['p(95)<300'],
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
        'User-Agent': `k6-Performance-Test-1M-per-Hour/1.0 (Real User Sim; VU: ${__VU})`,
    };
}

// =========================================================================
// VIRTUAL USER LIFESTYLES (SCENARIOS) DARI FILE ORIGINAL
// =========================================================================

function readHeavyUser(userId) {
    const params = { headers: generateHeaders(userId) };

    const res = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
    balanceLatency.add(res.timings.duration);
    const success = check(res, { 
        'is status 200': (r) => r.status === 200,
        'has balance data': (r) => r.body && r.body.includes('balance') 
    });
    handleResult(success, res);

    if (Math.random() < 0.2) {
        const refreshRes = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
        balanceLatency.add(refreshRes.timings.duration);
        const refreshSuccess = check(refreshRes, { 'is status 200': (r) => r.status === 200 });
        handleResult(refreshSuccess, refreshRes);
    }
}

function activeTransactor(userId) {
    const params = { headers: generateHeaders(userId) };

    const balRes = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
    const balSuccess = check(balRes, { 'is status 200': (r) => r.status === 200 });
    handleResult(balSuccess, balRes);

    const type = Math.random() > 0.5 ? 'deposit' : 'withdraw';
    const amount = randomIntBetween(10, 5000); 

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

    if (success && txRes.json('id')) {
        const txId = txRes.json('id');
        const statusRes = http.get(http.url`${BASE_URL}/transactions/${txId}`, params, { tags: { name: 'get-transaction-status' } });
        const statusSuccess = check(statusRes, { 'is status 200 or 202': (r) => r.status === 200 || r.status === 202 });
        handleResult(statusSuccess, statusRes);
    }
}

function apiClientBot(userId) {
    const params = { headers: generateHeaders(userId) };

    for (let i = 0; i < 10; i++) {
        const res = http.get(http.url`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
        balanceLatency.add(res.timings.duration);
        const botSuccess = check(res, { 'is status 200': (r) => r.status === 200 });
        handleResult(botSuccess, res);
    }
}

// =========================================================================
// MAIN FUNCTION (ROUTER)
// =========================================================================

export default function () {
    const randomUserId = randomIntBetween(1, 100000);
    const randomBehavior = Math.random();

    if (randomBehavior < 0.6) {
        readHeavyUser(randomUserId);
    } else if (randomBehavior < 0.9) {
        activeTransactor(randomUserId);
    } else {
        apiClientBot(randomUserId);
    }
}
