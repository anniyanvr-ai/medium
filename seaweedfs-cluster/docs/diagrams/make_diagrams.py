#!/usr/bin/env python3
"""Render the architecture diagrams (SVG source + 2x PNG) used in the design document.

    python3 docs/diagrams/make_diagrams.py         # needs: pip install playwright
"""
from pathlib import Path

OUT = Path(__file__).resolve().parent
FONT = "DejaVu Sans, Arial, sans-serif"

# palette ---------------------------------------------------------------------
INK, MUTED, LINE, BG = "#1f2933", "#5f6b7a", "#9aa5b1", "#ffffff"
PRI, PRI_BG = "#1f5fa8", "#eaf2fb"          # primary DC
DR, DR_BG = "#0f7b6c", "#e7f5f2"            # DR DC
HOT, HOT_BG = "#c2571a", "#fdf0e6"          # SSD / hot
COLD, COLD_BG = "#3d4f8a", "#eceffa"        # HDD / cold / EC
CTL, CTL_BG = "#6b3fa0", "#f3edfa"          # control plane (masters, admin)
ACC, ACC_BG = "#2b6f3e", "#eaf5ec"          # access (S3/F5)
BAD, BAD_BG = "#b3261e", "#fdecea"
NEU_BG = "#f5f7fa"


class SVG:
    def __init__(self, w, h, title):
        self.w, self.h, self.parts = w, h, []
        self.parts.append(f'<rect width="{w}" height="{h}" fill="{BG}"/>')
        self.text(24, 38, title, 22, INK, weight="bold", anchor="start")

    def add(self, s):
        self.parts.append(s)

    def rect(self, x, y, w, h, fill=NEU_BG, stroke=LINE, rx=8, dash=None, sw=1.5):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
                 f'stroke="{stroke}" stroke-width="{sw}"{d}/>')

    def text(self, x, y, s, size=13, color=INK, weight="normal", anchor="middle", italic=False):
        st = ' font-style="italic"' if italic else ""
        s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        self.add(f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" fill="{color}" '
                 f'font-weight="{weight}" text-anchor="{anchor}"{st}>{s}</text>')

    def box(self, x, y, w, h, title, sub=None, color=INK, fill=NEU_BG, size=13, sub2=None, dash=None):
        self.rect(x, y, w, h, fill, color, dash=dash)
        lines = [l for l in (sub, sub2) if l]
        ty = y + h / 2 + 5 - 8 * len(lines)
        self.text(x + w / 2, ty, title, size, color, "bold")
        for i, l in enumerate(lines):
            self.text(x + w / 2, ty + 17 * (i + 1), l, size - 2, MUTED)

    def group(self, x, y, w, h, label, color, fill, dash=None):
        self.rect(x, y, w, h, fill, color, rx=12, dash=dash, sw=2)
        self.text(x + 14, y + 22, label, 14, color, "bold", "start")

    def arrow(self, x1, y1, x2, y2, color=MUTED, label=None, dash=None, both=False, w=1.8, lx=None, ly=None,
              lcolor=None, lsize=11):
        mid = f"a{abs(hash(color)) % 10**6}"
        self.defs(mid, color)
        d = f' stroke-dasharray="{dash}"' if dash else ""
        ms = f' marker-start="url(#{mid}s)"' if both else ""
        self.add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{w}"{d} '
                 f'marker-end="url(#{mid})"{ms}/>')
        if label:
            self.text(lx if lx is not None else (x1 + x2) / 2 + 6, ly if ly is not None else (y1 + y2) / 2 - 6,
                      label, lsize, lcolor or color, anchor="start" if lx is None else "middle")

    def path(self, d, color=MUTED, dash=None, w=1.8, arrow=True):
        mid = f"a{abs(hash(color)) % 10**6}"
        self.defs(mid, color)
        da = f' stroke-dasharray="{dash}"' if dash else ""
        me = f' marker-end="url(#{mid})"' if arrow else ""
        self.add(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{w}"{da}{me}/>')

    _defs = set()

    def defs(self, mid, color):
        if mid in self._defs:
            return
        self._defs.add(mid)
        self.parts.insert(0, f'<defs><marker id="{mid}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                             f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
                             f'fill="{color}"/></marker><marker id="{mid}s" viewBox="0 0 10 10" refX="1" refY="5" '
                             f'markerWidth="7" markerHeight="7" orient="auto"><path d="M10,0 L0,5 L10,10 z" '
                             f'fill="{color}"/></marker></defs>')

    def legend(self, x, y, items):
        for i, (c, f, label) in enumerate(items):
            self.rect(x, y + i * 22, 16, 14, f, c, rx=3)
            self.text(x + 24, y + i * 22 + 12, label, 12, MUTED, anchor="start")

    def save(self, name):
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
               f'viewBox="0 0 {self.w} {self.h}">' + "".join(self.parts) + "</svg>")
        SVG._defs = set()
        (OUT / f"{name}.svg").write_text(svg)
        return name


# =============================================================================
def d1_overall():
    s = SVG(1600, 1090, "1. Logical architecture - Primary (active) + DR (standby), SeaweedFS Enterprise")
    s.box(620, 60, 360, 56, "Applications (S3 SDK / AWS CLI)", "one endpoint: s3.company.com", INK, "#fff")
    s.box(620, 150, 360, 56, "Global DNS / GSLB", "health-based; manual or automated failover", INK, NEU_BG)
    s.arrow(800, 116, 800, 150)
    for ox, name, col, bg, dc, net, vip, st in ((40, "PRIMARY DC1 (ACTIVE)", PRI, PRI_BG, "dc1", "10.10", "s3-primary", "normal traffic"),
                                                  (830, "DR DC2 (STANDBY)", DR, DR_BG, "dc2", "10.20", "s3-dr", "no app traffic until failover")):
        s.group(ox, 240, 730, 690, f"{name}", col, bg)
        s.box(ox + 215, 280, 300, 52, f"F5 VIP {vip}", f"{net}.5.100:443 - TLS, 2 monitors", ACC, ACC_BG)
        for i in range(3):
            x = ox + 40 + i * 225
            s.box(x, 370, 200, 52, f"S3 gateway {i+1}", f"{net}.5.1{i+1}:8333  stateless", ACC, "#fff")
            s.arrow(ox + 365, 332, x + 100, 370, ACC)
            s.box(x, 460, 200, 52, f"Filer {i+1}", f"{net}.4.1{i+1}:8888 / gRPC 18888", PRI if col == PRI else DR, "#fff")
            s.arrow(x + 100, 422, x + 100, 460, MUTED)
            s.box(x, 550, 200, 52, f"Master {i+1}", f"{net}.1.1{i+1}:9333  Raft", CTL, CTL_BG)
        s.text(ox + 365, 535, "metadata sync between filers (peer subscribe)", 11, MUTED, italic=True)
        s.text(ox + 365, 626, "3-node Raft quorum - independent per DC", 11, CTL, italic=True)
        s.group(ox + 20, 650, 335, 170, "HOT tier - SSD/NVMe", HOT, HOT_BG)
        s.text(ox + 187, 700, "3 nodes x NVMe, 1 per rack", 13, INK, "bold")
        s.text(ox + 187, 722, "landing tier for ALL new writes", 12, MUTED)
        s.text(ox + 187, 744, "replication 010 = 2 copies,", 12, MUTED)
        s.text(ox + 187, 762, "2 different racks", 12, MUTED)
        s.text(ox + 187, 794, "age 0 - 90 days", 13, HOT, "bold")
        s.group(ox + 375, 650, 335, 170, "COLD tier - HDD", COLD, COLD_BG)
        s.text(ox + 542, 700, "3+ nodes x 3 HDD (one -dir per disk)", 13, INK, "bold")
        s.text(ox + 542, 722, "erasure coded volumes", 12, MUTED)
        s.text(ox + 542, 744, "EC k+m sized to failure domains", 12, MUTED)
        s.text(ox + 542, 762, "(see diagram 5)", 12, MUTED)
        s.text(ox + 542, 794, "age > 90 days", 13, COLD, "bold")
        s.arrow(ox + 355, 735, ox + 375, 735, COLD)
        s.box(ox + 20, 842, 690, 62, "Admin + plugin workers", f"{net}.6.11:23646 - volume_tiering, ec_vacuum, balance"
              + (", data_movement" if col == PRI else ""), CTL, CTL_BG)
        s.text(ox + 365, 922, st, 11, col, italic=True)
    s.arrow(980, 178, 1195, 280, DR, dash="6 4", label="failover only", lx=1130, ly=220)
    s.arrow(620, 178, 405, 280, PRI, label="normal", lx=470, ly=220)
    # Data Movement: primary filer metadata log -> DR filer gRPC (worker runs on primary admin)
    s.arrow(730, 486, 870, 486, DR, w=3)
    s.text(800, 470, "Data", 12, DR, "bold")
    s.text(800, 512, "Movement", 12, DR, "bold")
    s.text(800, 962, "Enterprise Data Movement - CONTINUOUS (Primary filer log -> DR filer gRPC 18888, mTLS)", 13, DR, "bold")
    s.text(800, 982, "initial snapshot + tail; deletes propagated; lag = RPO metric", 11, MUTED)
    s.box(1040, 1005, 360, 64, "Backup target (separate)", "Data Movement BACKUP: deletes OFF", COLD, "#fff",
          sub2="object lock / immutable - protects against bad deletes")
    s.path("M 405 930 C 405 1037, 700 1037, 1040 1037", COLD, dash="5 4")
    s.box(20, 60, 250, 70, "Prometheus + Grafana", "scrapes every -metricsPort", INK, "#fff", sub2="Alertmanager -> on-call")
    return s.save("01-logical-architecture")


def d2_racks():
    s = SVG(1600, 820, "2. Primary DC physical layout - 3 racks = 3 failure domains (DR mirrors this with 10.20.x.x)")
    cols = [("Rack r1", "1"), ("Rack r2", "2"), ("Rack r3", "3")]
    for i, (rk, n) in enumerate(cols):
        x = 40 + i * 400
        s.group(x, 70, 370, 640, rk, PRI, PRI_BG)
        items = [("primary-master-0" + n, f"10.10.1.1{n}", "master :9333 / 19333 / m9324", CTL, CTL_BG),
                 ("primary-filer-0" + n, f"10.10.4.1{n}", "filer :8888 / 18888 / m9327", PRI, "#fff"),
                 ("primary-s3-0" + n, f"10.10.5.1{n}", "s3 :8333 / 18333 / m9328", ACC, ACC_BG),
                 ("primary-hot-0" + n, f"10.10.10.1{n}", "volume ssd :8080 / 18080 / m9325", HOT, HOT_BG),
                 ("primary-cold-0" + n, f"10.10.20.1{n}", "volume hdd :8081 / 18081 / m9326", COLD, COLD_BG)]
        if n == "1":
            items.append(("primary-admin", "10.10.6.11", "admin :23646 / 33646 + workers", CTL, CTL_BG))
        for j, (h, ip, svc, c, f) in enumerate(items):
            s.box(x + 20, 110 + j * 96, 330, 76, f"{h}  {ip}", svc, c, f)
    s.group(1250, 70, 330, 640, "What each failure costs", INK, "#fff")
    rows = [("1 S3 gateway", "none - F5 drains it", ACC),
            ("1 filer", "none - S3 uses other 2", PRI),
            ("1 master (incl. leader)", "none - re-election ~2-5s", CTL),
            ("1 hot node", "none - replica in other rack", HOT),
            ("1 cold node, EC 10+4", "EC DATA UNREADABLE (5 > 4)", BAD),
            ("whole rack", "hot OK; cold EC unreadable", BAD),
            ("admin host", "tiering/DM paused, no data loss", CTL),
            ("DC", "GSLB -> DR (RPO = DM lag)", DR)]
    for j, (a, b, c) in enumerate(rows):
        y = 118 + j * 72
        s.text(1268, y, a, 13, INK, "bold", "start")
        s.text(1268, y + 20, b, 12, c, anchor="start")
    s.text(40, 750, "Ports shown as HTTP / gRPC / metrics. gRPC is always HTTP+10000; metrics ports are in the 9324-9331 "
           "block so they can never collide with gRPC (v1.0 defect).", 12, MUTED, anchor="start")
    s.text(40, 772, "Every role is rack-labelled (-rack=rN, -dataCenter=dc1) so replica and shard placement follows "
           "the physical failure domains.", 12, MUTED, anchor="start")
    return s.save("02-rack-layout")


def d3_datapath():
    s = SVG(1600, 720, "3. Request data path - PUT and GET (application never sees Hot vs Cold)")
    lanes = [("Application", INK, "#fff"), ("F5 VIP", ACC, ACC_BG), ("S3 gateway", ACC, "#fff"),
             ("Filer (metadata)", PRI, PRI_BG), ("Master (leader)", CTL, CTL_BG),
             ("Hot volume rN", HOT, HOT_BG), ("Hot volume rM", HOT, HOT_BG), ("Cold EC shards", COLD, COLD_BG)]
    xs = []
    for i, (n, c, f) in enumerate(lanes):
        x = 40 + i * 195
        xs.append(x + 80)
        s.box(x, 60, 160, 44, n, None, c, f)
        s.add(f'<line x1="{x+80}" y1="104" x2="{x+80}" y2="690" stroke="{LINE}" stroke-dasharray="3 4"/>')
    steps = [(0, 1, "PUT /archive/2026/..xml", ACC), (1, 2, "pool member (least conn)", ACC),
             (2, 3, "assign + path rule / tier class", PRI), (3, 4, "Assign(collection, ssd, 010)", CTL),
             (4, 3, "fid + JWT + 2 locations", CTL), (2, 5, "chunk upload (JWT)", HOT),
             (5, 6, "synchronous replica (rack M)", HOT), (6, 5, "ack", HOT), (5, 2, "201 after BOTH copies", HOT),
             (2, 3, "CreateEntry (chunks, md5)", PRI), (2, 0, "200 OK  ETag", ACC)]
    y = 140
    for a, b, label, c in steps:
        s.arrow(xs[a], y, xs[b], y, c)
        s.text((xs[a] + xs[b]) / 2, y - 6, label, 11, c)
        y += 30
    y += 10
    s.text(40, y, "GET", 14, INK, "bold", "start")
    gets = [(0, 2, "GET key", ACC), (2, 3, "LookupEntry -> chunk fids", PRI), (2, 4, "LookupVolume (cached)", CTL),
            (2, 5, "read chunk  (replicated: any copy)", HOT), (2, 7, "or EC: read shards, rebuild if <= m missing", COLD),
            (2, 0, "200 stream", ACC)]
    y += 20
    for a, b, label, c in gets:
        s.arrow(xs[a], y, xs[b], y, c)
        s.text((xs[a] + xs[b]) / 2, y - 6, label, 11, c)
        y += 30
    return s.save("03-data-path")


def d4_lifecycle():
    s = SVG(1600, 560, "4. Data lifecycle - Enterprise Volume Tiering (demotion-only ladder)")
    s.box(40, 90, 260, 110, "S3 PUT", "server-side classification", ACC, ACC_BG, 15, sub2="selector ** -> class archive-data")
    s.box(380, 90, 330, 110, "Tier 0 - LANDING", "SSD / NVMe, replicated 010", HOT, HOT_BG, 15,
          sub2="2 copies in 2 racks, writable volumes")
    s.box(820, 90, 330, 110, "Tier 1 - cold-ec", "HDD, erasure coded k+m", COLD, COLD_BG, 15,
          sub2="read-only, ~1.4x overhead (10+4)")
    s.box(1260, 90, 300, 110, "(optional) Tier 2", "remote / object-lock backend", MUTED, NEU_BG, 15,
          sub2="[storage.backend] in master.toml", dash="5 4")
    s.arrow(300, 145, 380, 145, ACC)
    s.arrow(710, 145, 820, 145, COLD, w=3)
    s.text(765, 84, "quietForSeconds", 12, COLD, "bold")
    s.text(765, 180, "7,776,000 (90 d)", 12, COLD)
    s.arrow(1150, 145, 1260, 145, MUTED, dash="5 4")
    s.path("M 985 200 C 985 270, 545 270, 545 205", MUTED, dash="4 4")
    s.text(765, 262, "promotion = operator action only (ec.decode / volume.tier.move)", 12, MUTED, italic=True)
    s.group(40, 300, 700, 230, "How 'age' is measured (per VOLUME, not per object)", INK, "#fff")
    for i, t in enumerate(["Replicated volume: newest ModifiedAtSecond across its replicas.",
                           "One late write into a volume restarts its 90-day clock.",
                           "EC volume: time since last (re)encode - EC vacuum resets it.",
                           "Objects written on DR by Data Movement are NEW writes there:",
                           "the DR seed sits on DR SSD for 90 days unless the DR profile differs."]):
        s.text(60, 345 + i * 34, "- " + t, 13, BAD if i >= 3 else INK, anchor="start")
    s.group(780, 300, 780, 230, "Guard rails to configure", INK, "#fff")
    for i, t in enumerate(["pressure rule on ssd: high 80% -> demote coldest volumes to 70%",
                           "never EC or remote-tier the metadata class",
                           "worker pool on dedicated disks (-workingDir) - EC staging needs space",
                           "enable ec_vacuum for delete-heavy collections",
                           "test with a 300 s profile on a TEST collection before 7,776,000 s"]):
        s.text(800, 345 + i * 34, "- " + t, 13, INK, anchor="start")
    return s.save("04-tiering-lifecycle")


def d5_ec():
    s = SVG(1600, 700, "5. Erasure-coding placement vs failure domains - why EC 10+4 on 3 cold nodes is unsafe")

    def layout(x0, title, nodes, color, verdict, vcol, k, m):
        s.group(x0, 70, 740, 520, title, color, "#fff")
        per = [0] * nodes
        for i in range(k + m):
            per[i % nodes] += 1
        w = (700 - (nodes - 1) * 14) / nodes
        for n in range(nodes):
            x = x0 + 20 + n * (w + 14)
            dead = n == 0
            s.rect(x, 110, w, 330, BAD_BG if dead else COLD_BG, BAD if dead else COLD, 8)
            s.text(x + w / 2, 134, f"cold-0{n+1}" + ("  (FAILED)" if dead else ""), 13, BAD if dead else COLD, "bold")
            idx = [i for i in range(k + m) if i % nodes == n]
            for j, sh in enumerate(idx):
                data = sh < k
                s.rect(x + 14, 150 + j * 46, w - 28, 36, "#fff", COLD if data else HOT, 5)
                s.text(x + w / 2, 173 + j * 46, f"{'d' if data else 'p'}{sh}", 12, COLD if data else HOT, "bold")
            s.text(x + w / 2, 430, f"{per[n]} shards", 12, MUTED)
        s.text(x0 + 370, 480, f"lose 1 node = lose {per[0]} shards; tolerance = {m} parity", 14, INK, "bold")
        s.text(x0 + 370, 510, verdict, 15, vcol, "bold")
    layout(40, "v1.0 design: EC 10+4 on 3 cold nodes / 3 racks", 3, BAD, "UNREADABLE until node returns", BAD, 10, 4)
    layout(820, "Option A: EC 10+4 on 4+ cold nodes (4 racks)", 4, ACC, "READABLE (rebuilds from the 10 remaining)",
           ACC, 10, 4)
    s.text(40, 630, "Option B (keep 3 racks): Enterprise custom EC 6+3 -> 3 shards per rack <= 3 parity; survives a "
           "node or rack loss at 1.5x overhead.", 13, INK, anchor="start")
    s.text(40, 654, "Option C: 4+2 (2 per rack) at 1.5x overhead. Lab (F03): with 1 of 3 cold nodes down, 52/300 demoted "
           "objects were unreadable; a hand-run encode also left up to 12 shards of one volume on one node.", 13, INK, anchor="start")
    s.legend(1360, 610, [(COLD, "#fff", "data shard"), (HOT, "#fff", "parity shard")])
    return s.save("05-ec-placement")


def d6_dr():
    s = SVG(1600, 760, "6. DR replication, failover and failback - state machine")
    st = [(60, 100, "NORMAL", "Primary ACTIVE, DR STANDBY", "DM continuous P->DR", PRI, PRI_BG),
          (620, 100, "PRIMARY LOST", "declare disaster (runbook step 1-3)", "record last replicated ts", BAD, BAD_BG),
          (1180, 100, "DR ACTIVE", "GSLB -> DR VIP", "DR serves R/W, same creds", DR, DR_BG),
          (1180, 430, "PRIMARY REBUILT", "Primary back, isolated from apps", "reverse DM DR->P since ts", CTL, CTL_BG),
          (620, 430, "CONVERGED", "MIGRATE mode drain fence passed", "diff = 0 (listing + ETag)", ACC, ACC_BG),
          (60, 430, "CUT BACK", "freeze writes, GSLB -> Primary", "re-enable P->DR continuous", PRI, PRI_BG)]
    for x, y, t, a, b, c, f in st:
        s.box(x, y, 360, 120, t, a, c, f, 16, sub2=b)
    s.arrow(420, 160, 620, 160, BAD, label="DC failure", lx=520, ly=150)
    s.arrow(980, 160, 1180, 160, DR, label="activate DR", lx=1080, ly=150)
    s.arrow(1360, 220, 1360, 430, CTL, label="repair", lx=1400, ly=330)
    s.arrow(1180, 490, 980, 490, ACC, label="catch up", lx=1080, ly=480)
    s.arrow(620, 490, 420, 490, PRI, label="controlled cutover", lx=520, ly=480)
    s.arrow(240, 430, 240, 220, PRI, label="resume", lx=285, ly=330)
    s.group(60, 590, 1480, 150, "Measured in the lab (OSS filer.sync as Data Movement stand-in, 17 services per DC)",
            INK, "#fff")
    s.text(80, 640, "Continuous lag p50 0.22 s / max 0.32 s   |   delete propagation 0.4 s   |   DC crash under load: 1 of 470 "
           "acknowledged writes (last 10 ms) missing on DR   |   technical RTO 14 s", 13, INK, anchor="start")
    s.text(80, 668, "Seed by metadata-log replay silently missed 72-78% of existing objects -> seed must be a snapshot "
           "and must be proven with a listing/ETag diff (s3-reconcile)", 13, BAD, anchor="start")
    s.text(80, 696, "Failback: replay DR-era changes only (fromTsMs = failover - margin), reconcile primary-only tail "
           "writes (RPO window) before mirror deletes resume", 13, BAD, anchor="start")
    return s.save("06-dr-state-machine")


def d7_network():
    s = SVG(1640, 740, "7. Network flow matrix - default deny, open only these (source -> destination port)")
    dests = [("F5 VIP", "443"), ("S3 gw", "8333"), ("Filer", "8888 / 18888"), ("Master", "9333 / 19333"),
             ("Volume", "8080-8081 / +10000"), ("Admin", "23646 / 33646"), ("Metrics", "9324-9331"),
             ("DR filer", "18888"), ("DR volume", "8080-8081")]
    srcs = ["Application", "F5 self-IPs", "S3 gateways", "Filers", "Masters", "Volume servers",
            "Admin + workers", "Prometheus", "Ops bastion (mTLS)", "DM worker (primary)"]
    A, N, X = "allow", None, "deny"
    m = {
        "Application": [A, X, X, X, X, X, X, X, X],
        "F5 self-IPs": [N, A, X, X, X, X, X, X, X],
        "S3 gateways": [N, N, A, "19333", A, X, N, X, X],
        "Filers": [N, N, "peer 18888", A, A, X, N, X, X],
        "Masters": [N, N, N, "raft", "18080-1", X, N, X, X],
        "Volume servers": [N, N, N, "19333 hb", "repl/EC", X, N, X, X],
        "Admin + workers": [N, N, A, A, A, "workers", N, X, X],
        "Prometheus": [N, N, N, N, N, N, A, N, N],
        "Ops bastion (mTLS)": [N, N, "18888", "19333", N, "23646", A, N, N],
        "DM worker (primary)": [N, N, A, N, A, A, N, "mTLS", A],
    }
    x0, y0, cw, rh, lw = 30, 110, 150, 52, 230
    for j, (d, p) in enumerate(dests):
        x = x0 + lw + j * cw
        s.rect(x, y0 - 60, cw - 6, 54, NEU_BG, LINE, 6)
        s.text(x + (cw - 6) / 2, y0 - 36, d, 13, INK, "bold")
        s.text(x + (cw - 6) / 2, y0 - 16, p, 11, MUTED)
    for i, src in enumerate(srcs):
        y = y0 + i * rh
        s.rect(x0, y, lw - 6, rh - 6, NEU_BG, LINE, 6)
        s.text(x0 + 12, y + 29, src, 13, INK, "bold", "start")
        for j, v in enumerate(m[src]):
            x = x0 + lw + j * cw
            if v is None:
                s.rect(x, y, cw - 6, rh - 6, "#fafbfc", "#e4e7eb", 6)
                continue
            deny = v == X
            s.rect(x, y, cw - 6, rh - 6, BAD_BG if deny else ACC_BG, BAD if deny else ACC, 6)
            s.text(x + (cw - 6) / 2, y + 29, "DENY" if deny else ("ALLOW" if v == A else v), 12,
                   BAD if deny else ACC, "bold")
    y = y0 + len(srcs) * rh + 20
    s.legend(x0, y, [(ACC, ACC_BG, "allowed (label = specific port when narrower than the column)"),
                     (BAD, BAD_BG, "explicitly denied - tested: filer 8888 and master 9333 bypass S3 IAM (D04/D05)"),
                     ("#e4e7eb", "#fafbfc", "not needed - default deny")])
    s.text(x0 + 760, y + 12, "Cross-DC: the Data Movement sink uploads chunks straight to DR volume servers,", 12,
           INK, anchor="start")
    s.text(x0 + 760, y + 34, "so DR volume ports must be open to the primary worker hosts (or proxy via filer).", 12,
           INK, anchor="start")
    s.text(x0 + 760, y + 56, "Admin 4.47 binds worker gRPC 33646 on ALL interfaces - firewall it.", 12, BAD,
           anchor="start")
    return s.save("07-network-zones")


def d8_pipeline():
    s = SVG(1600, 520, "8. Deployment pipeline - one inventory, gated phases, automated acceptance")
    steps = [("inventory.yaml", "single source of truth", INK, "#fff"),
             ("swctl validate", "EC, racks, ports, sizing", CTL, CTL_BG),
             ("swctl render", "units, configs, PKI", PRI, PRI_BG),
             ("swctl install", "binary sha256, user, sysctl", PRI, PRI_BG),
             ("swctl deploy", "phase + health gate", ACC, ACC_BG),
             ("swctl bootstrap", "bucket, landing, tiering", HOT, HOT_BG),
             ("deploy --phase sync", "DR replication last", DR, DR_BG),
             ("e2e_test.py", "38 acceptance tests", BAD, BAD_BG)]
    for i, (t, sub, c, f) in enumerate(steps):
        x = 30 + i * 196
        s.box(x, 80, 176, 90, t, sub, c, f, 13)
        if i:
            s.arrow(x - 20, 125, x, 125, MUTED)
    ph = [("master", "Raft leader + all peers", CTL), ("volume", "DC / rack / disk", HOT),
          ("filer", "one at a time", PRI), ("s3", "/healthz on all", ACC), ("admin", "process up", CTL),
          ("worker", "process up", CTL)]
    s.group(620, 220, 940, 150, "swctl deploy - each phase must pass its gate before the next starts", ACC, "#fff")
    for i, (p, g, c) in enumerate(ph):
        x = 640 + i * 152
        s.box(x, 262, 138, 80, p, g, c, "#fff", 13)
        if i:
            s.arrow(x - 14, 302, x, 302, MUTED)
    s.path("M 902 170 L 902 220", ACC)
    s.group(30, 220, 560, 270, "Same code, two targets", INK, "#fff")
    for i, t in enumerate(["production.yaml  -> transport ssh, runtime systemd",
                           "lab.yaml (derived) -> transport local, loopback 127.x.y.z",
                           "identical flags, ports, racks, TLS, JWT, IAM",
                           "lab: 2 DCs x 17 services in ~15 s incl. gates",
                           "re-run = no-op (idempotent), gates re-checked",
                           "stop order = reverse phases"]):
        s.text(50, 268 + i * 34, "- " + t, 13, INK, anchor="start")
    s.group(620, 400, 940, 90, "Promote only when", BAD, "#fff")
    s.text(640, 446, "validate: 0 FAIL   |   e2e: 0 FAIL, every FINDING has an owner   |   Enterprise-only tests "
           "(F01, DM) passed on staging", 13, INK, anchor="start")
    return s.save("08-deployment-pipeline")


def d9_monitoring():
    s = SVG(1600, 520, "9. Observability - what is scraped and what pages someone")
    srcs = [("Masters x3", "m9324 raft, topology", CTL), ("Volumes x6", "m9325/9326 disk, IO", HOT),
            ("Filers x3", "m9327 metadata ops", PRI), ("S3 x3", "m9328 req/latency/code", ACC),
            ("Admin", "m9329 jobs", CTL), ("Workers", "m9330+ task outcome", CTL),
            ("Data Movement", "lag, phase, errors", DR), ("Synthetic probe", "HEAD canary via VIP", BAD)]
    for i, (t, sub, c) in enumerate(srcs):
        s.box(40, 70 + i * 54, 280, 44, t, sub, c, "#fff", 12)
        s.arrow(320, 92 + i * 54, 520, 290, LINE, w=1.2)
    s.box(520, 250, 220, 80, "Prometheus (x2, HA)", "15 s scrape, per DC", INK, NEU_BG)
    s.box(820, 150, 240, 70, "Grafana", "Primary / DR / Tiering / Replication", INK, "#fff")
    s.box(820, 370, 240, 70, "Alertmanager", "dedupe, route, silence", BAD, BAD_BG)
    s.arrow(740, 280, 820, 190)
    s.arrow(740, 300, 820, 400)
    s.group(1120, 60, 440, 420, "Critical (page)", BAD, "#fff")
    for i, t in enumerate(["master quorum < 2", "S3 healthy members < 2 (VIP at risk)",
                           "any EC volume missing > parity shards", "disk > 80% (ssd) / 85% (hdd)",
                           "DR lag > approved RPO for 5 min", "Data Movement policy failed",
                           "S3 5xx > 1% for 10 min", "canary HEAD fails via VIP",
                           "tiering / DM worker down > 15 min"]):
        s.text(1140, 110 + i * 38, "- " + t, 13, INK, anchor="start")
    return s.save("09-monitoring")


def d10_v22():
    s = SVG(1600, 1120, "10. v2.2 production architecture for 1M+ files/day (per DC; DR identical on 10.20.x.x)")
    s.box(620, 60, 360, 56, "Applications - 1M+ PUT/day, 1M+ GET/day", "keys: <prefix>/YYYY/MM/DD/HH/<id>", INK, "#fff")
    s.box(620, 140, 360, 50, "GSLB  s3.company.com", "TTL 60 s; manual DR decision", INK, NEU_BG)
    s.arrow(800, 116, 800, 140)
    s.group(40, 220, 1520, 860, "PRIMARY DC1 - 3 racks (r1 r2 r3)", PRI, PRI_BG)
    s.box(650, 255, 300, 50, "F5 VIP  (2 monitors)", "least-conn, slow-ramp", ACC, ACC_BG)
    s.arrow(800, 190, 800, 255, ACC)
    for i in range(3):
        x = 140 + i * 470
        s.box(x, 335, 380, 46, f"S3 gateway {i+1} (r{i+1})", "knows all 3 filers", ACC, "#fff", 12)
        s.arrow(800, 305, x + 190, 335, ACC, w=1.2)
        s.box(x, 405, 380, 46, f"Filer {i+1} (r{i+1})", "stateless - metadata in PostgreSQL", PRI, "#fff", 12)
        s.arrow(x + 190, 381, x + 190, 405, MUTED)
        s.arrow(x + 190, 451, 800, 490, PRI, w=1.2)
    s.box(560, 490, 480, 62, "PostgreSQL HA (postgres2 filer store) - CHANGED",
          "Patroni: leader + sync replica in 2 racks, VIP pg-primary", PRI, "#fff", 13, sub2="~380M entries/yr; PITR backups")
    for i in range(3):
        x = 140 + i * 470
        s.box(x, 580, 380, 40, f"Master {i+1} (r{i+1}) - Raft", None, CTL, CTL_BG, 12)
    s.group(70, 645, 700, 175, "HOT - 6 nodes (2/rack) x 3 NVMe 3.84 TB - CHANGED", HOT, HOT_BG)
    s.text(420, 700, "landing tier, replication 010 (2 racks)", 13, INK, "bold")
    s.text(420, 724, "sized: ingest x 90 d x 2 copies / 0.8", 12, MUTED)
    s.text(420, 746, "256 KB avg -> 57.6 TB needed, 69 TB raw", 12, MUTED)
    s.text(420, 790, "age 0-90 days", 13, HOT, "bold")
    s.group(830, 645, 700, 175, "COLD - 6 nodes (2/rack) x 3 HDD 16 TB - CHANGED", COLD, COLD_BG)
    s.text(1180, 700, "EC 6+3: 3 shards/rack = parity", 13, INK, "bold")
    s.text(1180, 724, "survives a node AND a rack loss (10+4 failed X04)", 12, MUTED)
    s.text(1180, 746, "275 d x 256 GB x 1.5 / 0.8 = 132 TB of 288 TB", 12, MUTED)
    s.text(1180, 790, "age > 90 days (volume_tiering worker)", 13, COLD, "bold")
    s.arrow(770, 735, 830, 735, COLD, w=2.5)
    s.box(70, 845, 330, 70, "Admin (r1)", "scheduler + UI only", CTL, CTL_BG, 13, sub2="restart is safe: not in data path")
    s.box(430, 845, 340, 70, "Worker host 1 (r2) - NEW", "data_movement, volume_tiering", CTL, "#fff", 13, sub2="2 worker processes")
    s.box(800, 845, 340, 70, "Worker host 2 (r3) - NEW", "data_movement, volume_tiering", CTL, "#fff", 13, sub2="2 worker processes")
    s.box(1170, 845, 360, 70, "sw-canary + sw-guard - NEW", "canary every 60 s, guard every 5 min", BAD, BAD_BG, 13,
          sub2="expire job / restart -> reconcile; 5-min sweep")
    s.arrow(400, 880, 430, 880, CTL)
    s.box(430, 945, 710, 64, "Data Movement CONTINUOUS -> DR filers (gRPC mTLS)  +  BACKUP policy (deletes OFF)",
          "tuned: slice 600 s, execution timeout 900 s  |  windowed s3-reconcile closes any gap", DR, DR_BG, 13)
    s.arrow(785, 915, 785, 945, DR, w=2.5)
    s.arrow(1350, 915, 1140, 975, BAD, dash="5 4", label="heals / reconciles", lx=1290, ly=960)
    s.text(800, 1050, "CHANGED/NEW vs v2.1: metadata store, EC code + cold node count, hot sizing, dedicated workers, "
           "DM tuning, canary + guard", 13, PRI, "bold")
    return s.save("10-v22-architecture")


def d11_guard():
    s = SVG(1600, 650, "11. sw-guard remediation loop (every 5 min on each admin host)")
    steps = [("1 canary", "PUT _canary/... on Primary\nGET on DR within RPO", DR),
             ("2 diagnose", "policy phase, job owner,\nworker liveness", CTL),
             ("3 heal", "expire orphaned job + detect;\nthen restart admin+workers", BAD),
             ("4 re-check", "canary must pass\n(<= 15 min budget)", DR),
             ("5 reconcile", "s3-reconcile --since last good\n--copy-missing (HEAD mode)", ACC)]
    for i, (t, sub, c) in enumerate(steps):
        x = 40 + i * 310
        a, b = sub.split("\n")
        s.box(x, 90, 270, 90, t, a, c, "#fff", 15, sub2=b)
        if i:
            s.arrow(x - 40, 135, x, 135, MUTED)
    s.text(175, 210, "if it passes: record 'last good' time, exit 0", 12, ACC)
    s.group(40, 260, 1520, 330, "Other checks in the same run", INK, NEU_BG)
    rows = [("tier  (E-03)", "EC set complete but replicated copy still on SSD", "lock; volume.delete leftover; verify"),
            ("class (E-02)", "volumes of a tiered collection without class", "report (never demoted)"),
            ("layout (E-07)", "master writable list has EC'd / missing volume ids", "rolling master restart, leader last"),
            ("dm    (E-10)", "enabled policy without a running job", "expire + detection, then restart ladder"),
            ("sweep (E-13)", "objects since last sweep missing on DR (HEAD)", "copy what Data Movement skipped"),
            ("capacity (E-12)", "free volume slots per tier / rack", "page before the hot tier is full")]
    for i, (a, b, c) in enumerate(rows):
        y = 312 + i * 44
        s.text(70, y, a, 14, INK, "bold", "start")
        s.text(280, y, b, 13, MUTED, anchor="start")
        s.text(1000, y, "-> " + c, 13, BAD, anchor="start")
    s.text(800, 620, "exit 0 = healthy, 2 = problem left (page); --json for monitoring; every fix re-checked before it is reported", 12, MUTED)
    return s.save("11-guard-loop")


if __name__ == "__main__":
    names = [f() for f in (d1_overall, d2_racks, d3_datapath, d4_lifecycle, d5_ec, d6_dr, d7_network,
                           d8_pipeline, d9_monitoring, d10_v22, d11_guard)]
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(device_scale_factor=2)
        for n in names:
            svg = (OUT / f"{n}.svg").read_text()
            w = int(svg.split('width="')[1].split('"')[0])
            h = int(svg.split('height="')[1].split('"')[0])
            pg.set_viewport_size({"width": w, "height": h})
            pg.set_content(f"<html><body style='margin:0'>{svg}</body></html>")
            pg.screenshot(path=str(OUT / f"{n}.png"), clip={"x": 0, "y": 0, "width": w, "height": h})
            print("rendered", n)
        b.close()
