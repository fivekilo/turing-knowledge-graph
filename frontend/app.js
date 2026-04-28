const data = window.KG_DATA || { nodes: [], edges: [], summary: {} };

const CLASS_COLORS = {
  Concept: "#d46a4f",
  Person: "#356c9b",
  Organization: "#2c8a6b",
  Place: "#7f5aa3",
  Field: "#b48528",
  Event: "#c74468",
  Work: "#4d6078",
  Machine: "#5f7e45",
  PublicationVenue: "#8d5b42",
  Entity: "#7c8691",
};

const nodes = data.nodes || [];
const edges = data.edges || [];
const summary = data.summary || {};
const nodeMap = new Map(nodes.map((node) => [node.id, node]));

const entityEdges = edges.filter((edge) => edge.object_type === "entity");
const literalEdges = edges.filter((edge) => edge.object_type !== "entity");

const outgoingEntityMap = new Map();
const incomingEntityMap = new Map();
const outgoingLiteralMap = new Map();

for (const edge of entityEdges) {
  if (!outgoingEntityMap.has(edge.subject)) outgoingEntityMap.set(edge.subject, []);
  if (!incomingEntityMap.has(edge.object)) incomingEntityMap.set(edge.object, []);
  outgoingEntityMap.get(edge.subject).push(edge);
  incomingEntityMap.get(edge.object).push(edge);
}

for (const edge of literalEdges) {
  if (!outgoingLiteralMap.has(edge.subject)) outgoingLiteralMap.set(edge.subject, []);
  outgoingLiteralMap.get(edge.subject).push(edge);
}

const state = {
  mode: "overview",
  search: "",
  classFilter: "ALL",
  selectedId: nodeMap.has("alan_turing") ? "alan_turing" : (nodes[0]?.id ?? null),
  transform: { scale: 1, x: 0, y: 0 },
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function classColor(classId) {
  return CLASS_COLORS[classId] || "#7c8691";
}

function renderHeroStats() {
  const host = document.getElementById("hero-stats");
  const items = [
    ["实体", summary.entities ?? nodes.length],
    ["关系", summary.relations ?? ""],
    ["三元组", summary.triples ?? edges.length],
  ];
  host.innerHTML = items
    .map(
      ([label, value]) => `
        <div class="stat">
          <div class="stat-label">${escapeHtml(label)}</div>
          <div class="stat-value">${escapeHtml(value)}</div>
        </div>
      `
    )
    .join("");
}

function renderModeSwitch() {
  const host = document.getElementById("mode-switch");
  const modes = [
    ["overview", "全图模式"],
    ["focus", "邻域模式"],
  ];
  host.innerHTML = modes
    .map(
      ([id, label]) => `
        <button class="mode-button ${state.mode === id ? "active" : ""}" data-mode="${id}">
          ${escapeHtml(label)}
        </button>
      `
    )
    .join("");

  host.querySelectorAll(".mode-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.mode = button.dataset.mode;
      resetZoom();
      renderModeSwitch();
      renderGraph();
    });
  });
}

function renderFilters() {
  const host = document.getElementById("class-filters");
  const classes = ["ALL", ...Object.keys(summary.entity_distribution || {}).sort()];
  host.innerHTML = classes
    .map((classId) => {
      const count = classId === "ALL" ? nodes.length : summary.entity_distribution?.[classId] ?? 0;
      const label = classId === "ALL" ? "全部" : classId;
      return `
        <button class="chip ${state.classFilter === classId ? "active" : ""}" data-class="${escapeHtml(classId)}">
          ${escapeHtml(label)} ${count}
        </button>
      `;
    })
    .join("");

  host.querySelectorAll(".chip").forEach((button) => {
    button.addEventListener("click", () => {
      state.classFilter = button.dataset.class;
      renderFilters();
      renderEntityList();
    });
  });
}

function filteredNodes() {
  const keyword = state.search.trim().toLowerCase();
  return nodes
    .filter((node) => state.classFilter === "ALL" || node.class_id === state.classFilter)
    .filter((node) => !keyword || node.label.toLowerCase().includes(keyword) || node.id.toLowerCase().includes(keyword))
    .sort((a, b) => a.label.localeCompare(b.label));
}

function renderEntityList() {
  const list = filteredNodes();
  document.getElementById("entity-count").textContent = `${list.length}/${nodes.length}`;
  const host = document.getElementById("entity-list");
  host.innerHTML = list
    .map(
      (node) => `
        <button class="entity-card ${state.selectedId === node.id ? "active" : ""}" data-id="${escapeHtml(node.id)}">
          <div class="entity-title">${escapeHtml(node.label)}</div>
          <div class="entity-meta">${escapeHtml(node.class_id)} · ${escapeHtml(node.id)}</div>
        </button>
      `
    )
    .join("");

  host.querySelectorAll(".entity-card").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedId = button.dataset.id;
      if (state.mode === "focus") resetZoom();
      renderAll();
    });
  });
}

function renderLegend() {
  const host = document.getElementById("graph-legend");
  host.innerHTML = Object.keys(summary.entity_distribution || {})
    .sort()
    .map(
      (classId) => `
        <span class="legend-item">
          <i class="legend-dot" style="background:${classColor(classId)}"></i>
          ${escapeHtml(classId)}
        </span>
      `
    )
    .join("");
}

function uniqueNeighborEdges(selectedId) {
  const outgoing = (outgoingEntityMap.get(selectedId) || []).map((edge) => ({ edge, direction: "out" }));
  const incoming = (incomingEntityMap.get(selectedId) || []).map((edge) => ({ edge, direction: "in" }));
  const seen = new Set();
  return [...outgoing, ...incoming].filter(({ edge, direction }) => {
    const key = `${direction}|${edge.subject}|${edge.predicate}|${edge.object}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function buildFocusGraph() {
  const centerId = state.selectedId;
  const centerNode = nodeMap.get(centerId);
  if (!centerNode) return { nodes: [], edges: [] };

  const neighborEdges = uniqueNeighborEdges(centerId).slice(0, 16);
  const graphNodes = new Map([[centerId, centerNode]]);
  const graphEdges = [];

  for (const item of neighborEdges) {
    const otherId = item.direction === "out" ? item.edge.object : item.edge.subject;
    const otherNode = nodeMap.get(otherId);
    if (otherNode) graphNodes.set(otherId, otherNode);
    graphEdges.push(item);
  }

  return { nodes: [...graphNodes.values()], edges: graphEdges };
}

function buildOverviewGraph() {
  return {
    nodes,
    edges: entityEdges.map((edge) => ({ edge, direction: "out" })),
  };
}

function groupNodesByClass(graphNodes) {
  const grouped = new Map();
  for (const node of graphNodes) {
    if (!grouped.has(node.class_id)) grouped.set(node.class_id, []);
    grouped.get(node.class_id).push(node);
  }
  return [...grouped.entries()].sort((a, b) => a[0].localeCompare(b[0]));
}

function layoutOverview(graphNodes, width, height) {
  const grouped = groupNodesByClass(graphNodes);
  const positions = new Map();
  const columns = Math.max(1, grouped.length);
  const colWidth = width / columns;

  grouped.forEach(([classId, members], classIndex) => {
    members.sort((a, b) => a.label.localeCompare(b.label));
    const rows = Math.ceil(members.length / 2);
    members.forEach((node, idx) => {
      const colOffset = idx % 2;
      const row = Math.floor(idx / 2);
      const x = classIndex * colWidth + colWidth * (colOffset === 0 ? 0.34 : 0.66);
      const y = 110 + row * Math.max(70, (height - 180) / Math.max(rows, 1));
      positions.set(node.id, { x, y });
    });
  });

  return positions;
}

function layoutFocus(graphNodes, width, height) {
  const positions = new Map();
  const center = { x: width / 2, y: height / 2 };
  const centerId = state.selectedId;
  positions.set(centerId, center);
  const others = graphNodes.filter((node) => node.id !== centerId);
  others.forEach((node, index) => {
    const angle = (-Math.PI / 2) + (index / Math.max(others.length, 1)) * Math.PI * 2;
    const radius = 250;
    positions.set(node.id, {
      x: center.x + Math.cos(angle) * radius,
      y: center.y + Math.sin(angle) * radius,
    });
  });
  return positions;
}

function renderGraph() {
  const svg = document.getElementById("graph-svg");
  const title = document.getElementById("graph-title");
  const subtitle = document.getElementById("graph-subtitle");

  const graph = state.mode === "overview" ? buildOverviewGraph() : buildFocusGraph();
  const width = 1200;
  const height = 760;

  title.textContent = state.mode === "overview" ? "完整知识图谱" : `${nodeMap.get(state.selectedId)?.label || ""} 的邻域图`;
  subtitle.textContent =
    state.mode === "overview"
      ? "展示当前全部实体和实体关系，可缩放、拖动，适合整体讲结构。"
      : "展示选中实体的一跳邻居，适合逐个讲实体关系。";

  const positions =
    state.mode === "overview"
      ? layoutOverview(graph.nodes, width, height)
      : layoutFocus(graph.nodes, width, height);

  const defs = `
    <defs>
      <marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="strokeWidth">
        <path d="M0,0 L8,4 L0,8 z" fill="rgba(46, 56, 66, 0.25)"></path>
      </marker>
    </defs>
  `;

  const edgeMarkup = graph.edges
    .map(({ edge, direction }) => {
      const fromId = direction === "out" ? edge.subject : edge.object;
      const toId = direction === "out" ? edge.object : edge.subject;
      const from = positions.get(fromId);
      const to = positions.get(toId);
      if (!from || !to) return "";
      const active =
        state.mode === "focus"
          ? edge.subject === state.selectedId || edge.object === state.selectedId
          : true;
      const dimmed = active ? "" : "dimmed";
      const midX = (from.x + to.x) / 2;
      const midY = (from.y + to.y) / 2;
      return `
        <g>
          <line class="graph-edge ${dimmed}" x1="${from.x}" y1="${from.y}" x2="${to.x}" y2="${to.y}" marker-end="url(#arrow)"></line>
          <text class="graph-edge-label ${dimmed}" x="${midX}" y="${midY - 6}">${escapeHtml(edge.predicate_label || edge.predicate)}</text>
        </g>
      `;
    })
    .join("");

  const nodeMarkup = graph.nodes
    .map((node) => {
      const point = positions.get(node.id);
      if (!point) return "";
      const isSelected = node.id === state.selectedId;
      const dimmed =
        state.mode === "focus" && !isSelected && !uniqueNeighborEdges(state.selectedId).some(({ edge }) => edge.subject === node.id || edge.object === node.id)
          ? "dimmed"
          : "";
      const radius = isSelected ? 36 : 24;
      const label = node.label.length > 18 ? `${node.label.slice(0, 16)}…` : node.label;
      return `
        <g class="graph-node ${isSelected ? "selected" : ""} ${dimmed}" transform="translate(${point.x}, ${point.y})" data-id="${escapeHtml(node.id)}">
          <circle r="${radius}" fill="${classColor(node.class_id)}"></circle>
          <text y="${isSelected ? 56 : 42}">${escapeHtml(label)}</text>
        </g>
      `;
    })
    .join("");

  svg.innerHTML = `${defs}<g id="graph-viewport">${edgeMarkup}${nodeMarkup}</g>`;
  applyTransform();

  svg.querySelectorAll(".graph-node").forEach((group) => {
    group.addEventListener("click", () => {
      const id = group.dataset.id;
      if (id && nodeMap.has(id)) {
        state.selectedId = id;
        if (state.mode === "focus") resetZoom();
        renderAll();
      }
    });
  });
}

function applyTransform() {
  const viewport = document.getElementById("graph-viewport");
  if (!viewport) return;
  viewport.setAttribute(
    "transform",
    `translate(${state.transform.x}, ${state.transform.y}) scale(${state.transform.scale})`
  );
}

function resetZoom() {
  state.transform = { scale: 1, x: 0, y: 0 };
  applyTransform();
}

function updateScale(nextScale) {
  state.transform.scale = Math.min(2.8, Math.max(0.45, nextScale));
  applyTransform();
}

function renderDetail() {
  const host = document.getElementById("entity-detail");
  const node = nodeMap.get(state.selectedId);
  if (!node) {
    host.innerHTML = `<div class="empty">当前没有可展示的实体。</div>`;
    return;
  }

  const literalFacts = outgoingLiteralMap.get(node.id) || [];
  const outgoing = outgoingEntityMap.get(node.id) || [];
  const incoming = incomingEntityMap.get(node.id) || [];

  host.innerHTML = `
    <div class="detail-hero">
      <div class="detail-key">${escapeHtml(node.class_id)}</div>
      <h3>${escapeHtml(node.label)}</h3>
      <p>${escapeHtml(node.description || "当前实体暂无详细描述。")}</p>
    </div>

    <div class="detail-block">
      <h4>属性</h4>
      <div class="detail-list">
        ${
          literalFacts.length
            ? literalFacts
                .map(
                  (edge) => `
                    <div class="detail-item">
                      <div class="detail-key">${escapeHtml(edge.predicate_label || edge.predicate)}</div>
                      <div class="detail-value">${escapeHtml(edge.object)}</div>
                    </div>
                  `
                )
                .join("")
            : `<div class="empty">没有属性事实。</div>`
        }
      </div>
    </div>

    <div class="detail-block">
      <h4>出边</h4>
      <div class="detail-list">
        ${
          outgoing.length
            ? outgoing
                .map((edge) => {
                  const target = nodeMap.get(edge.object);
                  return `
                    <div class="detail-item">
                      <div class="detail-key">${escapeHtml(edge.predicate_label || edge.predicate)}</div>
                      <div class="detail-value">${escapeHtml(target?.label || edge.object)}</div>
                    </div>
                  `;
                })
                .join("")
            : `<div class="empty">没有出边。</div>`
        }
      </div>
    </div>

    <div class="detail-block">
      <h4>入边</h4>
      <div class="detail-list">
        ${
          incoming.length
            ? incoming
                .map((edge) => {
                  const source = nodeMap.get(edge.subject);
                  return `
                    <div class="detail-item">
                      <div class="detail-key">${escapeHtml(edge.predicate_label || edge.predicate)}</div>
                      <div class="detail-value">${escapeHtml(source?.label || edge.subject)}</div>
                    </div>
                  `;
                })
                .join("")
            : `<div class="empty">没有入边。</div>`
        }
      </div>
    </div>
  `;
}

function bindGraphControls() {
  document.getElementById("zoom-in").addEventListener("click", () => updateScale(state.transform.scale * 1.15));
  document.getElementById("zoom-out").addEventListener("click", () => updateScale(state.transform.scale / 1.15));
  document.getElementById("zoom-reset").addEventListener("click", resetZoom);

  const svg = document.getElementById("graph-svg");
  let dragging = false;
  let start = { x: 0, y: 0 };

  svg.addEventListener("wheel", (event) => {
    event.preventDefault();
    const factor = event.deltaY < 0 ? 1.08 : 0.92;
    updateScale(state.transform.scale * factor);
  });

  svg.addEventListener("pointerdown", (event) => {
    dragging = true;
    svg.classList.add("dragging");
    start = { x: event.clientX - state.transform.x, y: event.clientY - state.transform.y };
  });

  window.addEventListener("pointermove", (event) => {
    if (!dragging) return;
    state.transform.x = event.clientX - start.x;
    state.transform.y = event.clientY - start.y;
    applyTransform();
  });

  window.addEventListener("pointerup", () => {
    dragging = false;
    svg.classList.remove("dragging");
  });
}

function bindSearch() {
  document.getElementById("search-input").addEventListener("input", (event) => {
    state.search = event.target.value || "";
    renderEntityList();
  });
}

function renderAll() {
  renderModeSwitch();
  renderFilters();
  renderEntityList();
  renderLegend();
  renderGraph();
  renderDetail();
}

renderHeroStats();
bindSearch();
bindGraphControls();
renderAll();
