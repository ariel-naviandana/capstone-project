import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
const transactionLatency = new Trend('tx_latency', true);
const balanceLatency = new Trend('balance_latency', true);
const errorRate = new Rate('errors');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const CHAOS_TARGET = __ENV.CHAOS_TARGET || 'none'; // postgres, redis, kafka, none

// Log chaos target di awal
console.log(`Chaos mode: ${CHAOS_TARGET} failure simulation`);

// =========================================================================
// K6 OPTIONS
// =========================================================================
export const options = {
    scenarios: {
        chaos_scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 100 }, // Ramp-up baseline
                { duration: '90s', target: 100 }, // Hold selama chaos/recovery
                { duration: '60s', target: 0 },   // Ramp-down
            ],
            gracefulRampDown: '30s',
        },
    },
    thresholds: {
        'http_req_duration': ['p(95)<1000'], // lebih longgar saat chaos
        'errors': ['rate<0.10'],             // tolerate sampai 10% error saat failure
    },
};

// =========================================================================
// HELPER & BEHAVIOR
// =========================================================================

function generateHeaders(userId) {
    return {
        'Content-Type': 'application/json',
        'X-User-ID': userId.toString(),
        'Accept': 'application/json',
        'User-Agent': `k6-Chaos-Test/1.0 (VU: ${__VU}, Chaos: ${CHAOS_TARGET})`,
    };
}

function chaosUser(userId) {
    const params = { headers: generateHeaders(userId) };

    // 70% GET balance (read)
    if (Math.random() < 0.7) {
        const res = http.get(`${BASE_URL}/users/${userId}/balance`, params, { tags: { name: 'get-balance' } });
        balanceLatency.add(res.timings.duration);
        check(res, { 'status 200 or 503': (r) => r.status === 200 || r.status === 503 });
        errorRate.add(res.status >= 500 && res.status !== 503);
        sleep(randomIntBetween(0.5, 1.5));
    }
    // 30% POST transaction (write)
    else {
        const type = Math.random() > 0.5 ? 'deposit' : 'withdraw';
        const amount = randomIntBetween(10, 1000);

        const payload = JSON.stringify({
            user_id: userId,
            amount: amount,
            type: type,
            description: `Chaos test ${type} (${CHAOS_TARGET})`,
        });

        const txRes = http.post(`${BASE_URL}/transactions`, payload, params, { tags: { name: 'post-transaction' } });
        transactionLatency.add(txRes.timings.duration);

        check(txRes, { 'status 202 or 503': (r) => r.status === 202 || r.status === 503 });
        errorRate.add(txRes.status >= 500 && txRes.status !== 503);
        sleep(randomIntBetween(1, 2));
    }
}

// =========================================================================
// MAIN FUNCTION
// =========================================================================

export default function () {
    const randomUserId = randomIntBetween(1, 10000);
    chaosUser(randomUserId);
}