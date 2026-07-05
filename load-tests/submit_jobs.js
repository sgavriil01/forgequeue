import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    submit_jobs: {
      executor: "shared-iterations",
      vus: Number(__ENV.VUS || 20),
      iterations: Number(__ENV.JOBS || 1000),
      maxDuration: __ENV.MAX_DURATION || "2m",
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
      message: `submit-jobs-vu-${__VU}-iter-${__ITER}`,
    },
    max_retries: 3,
  });

  const res = http.post(`${API_URL}/jobs`, payload, {
    headers: {
      "Content-Type": "application/json",
    },
  });

  check(res, {
    "job created": (r) => r.status === 201,
  });
}