"""
Minimal client for the SeaweedFS Enterprise Admin REST API (verified on 4.48-enterprise).

Used by the Enterprise acceptance tests to enable plugin job types, register Data Movement
targets/policies and read their status - the same calls the Admin UI makes.

    adm = AdminAPI("http://10.10.6.11:23646")            # lab: admin auth disabled on loopback
    adm.enable_job("data_movement", detection_interval_minutes=1)
    adm.put_target({...}); adm.put_policy({...}); adm.dm_status()
"""
import json
import urllib.parse
import re
import urllib.error
import urllib.request
from http.cookiejar import CookieJar


class AdminAPI:
    def __init__(self, base, user=None, password=None, timeout=30):
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
        self.csrf = ""
        if user:
            self._login(user, password)
        self._refresh_csrf()

    # -- plumbing --------------------------------------------------------- #
    def _login(self, user, password):
        data = urllib.parse.urlencode({"username": user, "password": password}).encode()
        self.op.open(urllib.request.Request(self.base + "/login", data=data), timeout=self.timeout)

    def _refresh_csrf(self):
        try:
            html = self.op.open(self.base + "/", timeout=self.timeout).read().decode(errors="replace")
            m = re.search(r'name="csrf-token" content="([^"]*)"', html)
            self.csrf = m.group(1) if m else ""
        except Exception:  # noqa: BLE001
            self.csrf = ""

    def req(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if self.csrf:
            r.add_header("X-CSRF-Token", self.csrf)
        try:
            with self.op.open(r, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw.strip() else {})
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except Exception:  # noqa: BLE001
                return e.code, {"error": raw.decode(errors="replace")[:300]}

    # -- plugin job types ------------------------------------------------- #
    def job_config(self, job_type):
        return self.req("GET", f"/api/plugin/job-types/{job_type}/config")[1]

    def enable_job(self, job_type, enabled=True, **runtime):
        cfg = self.job_config(job_type)
        desc = self.req("GET", f"/api/plugin/job-types/{job_type}/descriptor")[1]
        rt = dict(cfg.get("admin_runtime") or {})
        rt["enabled"] = enabled
        rt.update(runtime)
        payload = {"job_type": job_type,
                   "descriptor_version": desc.get("descriptor_version", cfg.get("descriptor_version", 0)),
                   "admin_runtime": rt,
                   "admin_config_values": cfg.get("admin_config_values") or {},
                   "worker_config_values": cfg.get("worker_config_values") or {}}
        return self.req("PUT", f"/api/plugin/job-types/{job_type}/config", payload)

    def set_values(self, job_type, admin=None, worker=None):
        cfg = self.job_config(job_type)
        desc = self.req("GET", f"/api/plugin/job-types/{job_type}/descriptor")[1]
        av = dict(cfg.get("admin_config_values") or {})
        wv = dict(cfg.get("worker_config_values") or {})
        av.update(admin or {})
        wv.update(worker or {})
        payload = {"job_type": job_type, "descriptor_version": desc.get("descriptor_version", 0),
                   "admin_runtime": cfg.get("admin_runtime"), "admin_config_values": av,
                   "worker_config_values": wv}
        return self.req("PUT", f"/api/plugin/job-types/{job_type}/config", payload)

    def detect(self, job_type):
        return self.req("POST", f"/api/plugin/job-types/{job_type}/detect", {})

    def run(self, job_type):
        return self.req("POST", f"/api/plugin/job-types/{job_type}/run", {})

    def expire_job(self, job_id, reason="expired by operator"):
        """Mark an active job failed so the scheduler can re-propose it (Admin UI 'Expire job')."""
        return self.req("POST", f"/api/plugin/jobs/{urllib.parse.quote(job_id, safe='')}/expire", {"reason": reason})

    def jobs(self, limit=500):
        return self.req("GET", f"/api/plugin/jobs?limit={limit}")[1]

    # -- data movement ---------------------------------------------------- #
    def dm_config(self):
        return self.req("GET", "/api/data-movement/config")[1]

    def _token(self, kind, name):
        for x in self.dm_config().get(kind, []) or []:
            if x.get("name") == name:
                return x.get("token", "")
        return ""

    def put_target(self, target):
        return self.req("POST", "/api/data-movement/targets",
                        {"token": self._token("targets", target["name"]), "target": target})

    def put_policy(self, policy):
        return self.req("POST", "/api/data-movement/policies",
                        {"token": self._token("policies", policy["name"]), "policy": policy})

    def delete_policy(self, name):
        return self.req("DELETE", f"/api/data-movement/policies/{name}")

    def dm_status(self):
        return self.req("GET", "/api/data-movement/status")[1]
