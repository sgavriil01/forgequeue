import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    create_jobs: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 20),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: Number(__ENV.VUS || 20),
      maxVUs: Number(__ENV.MAX_VUS || 100),
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

const API_URL = __ENV.API_URL || "http://localhost:8080";

export default function () {
  const payload = JSON.stringify({
    kind: "test_job",
    payload: {
      message: `load-test-vu-${__VU}-iter-${__ITER}`,
    },
    max_retries: 3,
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  const res = http.post(`${API_URL}/jobs`, payload, params);

  check(res, {
    "job created": (r) => r.status === 201,
  });
}