import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 10,             
    duration: '30s',     
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

    // Insert document
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
