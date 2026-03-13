import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        throughput_test: {
            executor: 'ramping-arrival-rate',
            startRate: 200,
            timeUnit: '1s',
            preAllocatedVUs: 500,
            maxVUs: 3000,
            stages: [
                { duration: '30s', target: 200 },
                { duration: '1m', target: 600 },
                { duration: '2m', target: 600 },
                { duration: '30s', target: 0 },
            ],
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500'],
    },
};

const BASE_URL = 'http://localhost:8000';

export default function () {
    const randomUserId = Math.floor(Math.random() * 10000) + 1;
    const params = {
        headers: {
            'Content-Type': 'application/json',
            'X-User-ID': randomUserId.toString(),
        },
    };

    // 1. Skenario Write-Heavy: Create Transaction
    const payload = JSON.stringify({
        user_id: randomUserId,
        amount: Math.random() * 1000 + 100,
        type: 'deposit',
        description: 'B.4 Peak Load Test',
    });

    const postRes = http.post(http.url`${BASE_URL}/transactions`, payload, params);

    check(postRes, {
        'post status is 202': (r) => r.status === 202,
    });

    // Ambil tx_id untuk testing read-heavy selanjutnya
    const txId = postRes.json() ? postRes.json().id : null;

    // 2. Skenario Read-Heavy: Get User Balance
    const balanceRes = http.get(http.url`${BASE_URL}/users/${randomUserId}/balance`, params);
    check(balanceRes, {
        'get balance status is 200': (r) => r.status === 200,
    });

    // 3. Skenario Read-Heavy: Get Transaction Status (jika tx_id tersedia)
    if (txId) {
        const statusRes = http.get(http.url`${BASE_URL}/transactions/${txId}`, params);
        check(statusRes, {
            'get status is 200 or 202': (r) => r.status === 200 || r.status === 202,
        });
    }

    sleep(1);
}