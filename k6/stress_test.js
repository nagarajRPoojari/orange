import http from 'k6/http';
import { check, sleep } from 'k6';


export const options = {
    stages: [
        { duration: '1m', target: 100 },   // Start low
        { duration: '1m', target: 300 },   // Increase
        { duration: '1m', target: 600 },   // Higher
        { duration: '1m', target: 1000 },  // Max load
        { duration: '2m', target: 0 },     // Cool down
    ],
    thresholds: {
        http_req_failed: ['rate<0.05'],   
        http_req_duration: ['p(95)<1000'], 
    },
};

const BASE_URL = 'http://localhost:8000';

export function setup() {
    console.log('Creating schema...');
    const res = http.post(BASE_URL, JSON.stringify({
        query: 'create document test {"name":"STRING"}'
    }), {
        headers: { 'Content-Type': 'application/json' }
    });
    check(res, { 'schema created': (r) => r.status === 200 });
}

export default function () {
    const id = Math.floor(Math.random() * 20) + 1;

    const insertRes = http.post(BASE_URL, JSON.stringify({
        query: `insert value into test {"_ID": ${id}, "name": "hello-${id}"}`
    }), {
        headers: { 'Content-Type': 'application/json' }
    });

    check(insertRes, {
        'insert succeeded': (r) => r.status === 200,
    });

    sleep(0.5); 

    const selectRes = http.post(BASE_URL, JSON.stringify({
        query: `select * from test where _ID = ${id}`
    }), {
        headers: { 'Content-Type': 'application/json' }
    });

    check(selectRes, {
        'search succeeded': (r) => r.status === 200,
        'response has result': (r) => r.body && r.body.includes(`"hello-${id}"`),
    });

    sleep(0.5);
}
