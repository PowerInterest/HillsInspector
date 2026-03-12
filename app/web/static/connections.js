(function () {
    "use strict";

    // ---- State ----
    const state = {
        nodes: [],
        links: [],
        simulation: null,
        svg: null,
        g: null,           // zoom group
        tooltip: null,
        expandedIds: new Set(),
        rootEntity: null,   // doc_number of searched entity
    };

    // ---- Constants ----
    const NODE_SIZE = { entity: 24, person: 14, property: 18 };
    const COLORS = {
        entity: "#2563eb",
        person: "#10b981",
        property: "#f59e0b",
    };

    // ---- Init ----
    function init() {
        setupSearch();
        setupSvg();
        setupTooltip();
        document.getElementById("reset-graph").addEventListener("click", resetGraph);
    }

    // ---- Search ----
    function setupSearch() {
        const input = document.getElementById("entity-search");
        const dropdown = document.getElementById("entity-results");
        let debounceTimer;

        input.addEventListener("input", function () {
            clearTimeout(debounceTimer);
            const q = input.value.trim();
            if (q.length < 3) {
                dropdown.style.display = "none";
                return;
            }
            debounceTimer = setTimeout(() => {
                fetch("/api/connections/search?q=" + encodeURIComponent(q))
                    .then(r => r.json())
                    .then(data => renderDropdown(data.results, dropdown))
                    .catch(err => console.error("Search error:", err));
            }, 300);
        });

        // Close dropdown on outside click
        document.addEventListener("click", function (e) {
            if (!e.target.closest(".connections-search-box")) {
                dropdown.style.display = "none";
            }
        });
    }

    function renderDropdown(results, dropdown) {
        if (!results || results.length === 0) {
            dropdown.innerHTML = '<div class="entity-result-item" style="color:#999">No matches</div>';
            dropdown.style.display = "block";
            return;
        }
        dropdown.innerHTML = results.map(r => `
            <div class="entity-result-item" data-doc="${r.doc_number}">
                <div class="entity-name">${esc(r.entity_name)}</div>
                <div class="entity-meta">${r.filing_type || ""} | ${r.status === "A" ? "Active" : "Inactive"} | Filed ${r.filed_date || "?"} | ${r.age_years != null ? r.age_years + "yr" : ""}</div>
            </div>
        `).join("");
        dropdown.style.display = "block";

        dropdown.querySelectorAll(".entity-result-item").forEach(item => {
            item.addEventListener("click", function () {
                const doc = this.dataset.doc;
                dropdown.style.display = "none";
                document.getElementById("entity-search").value = this.querySelector(".entity-name").textContent;
                loadEntity(doc, true);
            });
        });
    }

    // ---- SVG Setup ----
    function setupSvg() {
        const container = document.querySelector(".connections-graph-container");
        const svg = d3.select("#connections-graph");
        const width = container.clientWidth;
        const height = container.clientHeight;

        svg.attr("viewBox", [0, 0, width, height]);

        const g = svg.append("g");
        state.svg = svg;
        state.g = g;

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on("zoom", (event) => g.attr("transform", event.transform));
        svg.call(zoom);

        // Link and node groups (links behind nodes)
        g.append("g").attr("class", "links-group");
        g.append("g").attr("class", "nodes-group");
    }

    // ---- Tooltip ----
    function setupTooltip() {
        state.tooltip = d3.select(".connections-graph-container")
            .append("div")
            .attr("class", "graph-tooltip")
            .style("display", "none");
    }

    function showTooltip(event, d) {
        let html = "";
        if (d.type === "entity") {
            html = `<strong>${esc(d.label)}</strong>
                <div class="tooltip-row"><span class="tooltip-label">Status</span><span>${d.data.status === "A" ? "Active" : d.data.status === "I" ? "Inactive" : d.data.status || "?"}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Type</span><span>${d.data.filing_type || "?"}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Filed</span><span>${d.data.filed_date || "?"}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Age</span><span>${d.data.age_years != null ? d.data.age_years + " years" : "?"}</span></div>`;
        } else if (d.type === "person") {
            html = `<strong>${esc(d.label)}</strong>
                <div class="tooltip-row"><span class="tooltip-label">Role</span><span>${d.data.party_role || "?"}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Title</span><span>${d.data.party_title || "?"}</span></div>`;
        } else if (d.type === "property") {
            html = `<strong>${esc(d.data.address || d.label)}</strong>
                <div class="tooltip-row"><span class="tooltip-label">Folio</span><span>${d.data.folio}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Owner</span><span>${esc(d.data.owner_name || "?")}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Market Value</span><span>${d.data.market_value ? "$" + d.data.market_value.toLocaleString() : "?"}</span></div>
                <div class="tooltip-row"><span class="tooltip-label">Foreclosure</span><span>${d.data.in_foreclosure ? "Yes" : "No"}</span></div>`;
        }
        state.tooltip.html(html).style("display", "block");
        positionTooltip(event);
    }

    function positionTooltip(event) {
        const container = document.querySelector(".connections-graph-container");
        const rect = container.getBoundingClientRect();
        state.tooltip
            .style("left", (event.clientX - rect.left + 15) + "px")
            .style("top", (event.clientY - rect.top - 10) + "px");
    }

    function hideTooltip() {
        state.tooltip.style("display", "none");
    }

    // ---- Graph Data ----
    function nodeId(type, key) {
        return type + ":" + key;
    }

    function addNode(type, key, label, data) {
        const id = nodeId(type, key);
        if (state.nodes.find(n => n.id === id)) return id;
        const container = document.querySelector(".connections-graph-container");
        state.nodes.push({
            id, type, key, label, data,
            x: container.clientWidth / 2 + (Math.random() - 0.5) * 100,
            y: container.clientHeight / 2 + (Math.random() - 0.5) * 100,
        });
        return id;
    }

    function addLink(sourceId, targetId, label) {
        const exists = state.links.find(l =>
            (l.source.id || l.source) === sourceId && (l.target.id || l.target) === targetId
        );
        if (!exists) {
            state.links.push({ source: sourceId, target: targetId, label: label || "" });
        }
    }

    // ---- Load/Expand ----
    async function loadEntity(docNumber, isRoot) {
        if (state.expandedIds.has("entity:" + docNumber)) return;
        state.expandedIds.add("entity:" + docNumber);
        updateStatus("Loading entity...");

        try {
            const resp = await fetch("/api/connections/entity/" + encodeURIComponent(docNumber));
            const data = await resp.json();
            if (data.error) { updateStatus("Error: " + data.error); return; }

            const entId = addNode("entity", data.entity.doc_number, data.entity.entity_name, data.entity);
            if (isRoot) {
                state.rootEntity = docNumber;
                // Center root node
                const container = document.querySelector(".connections-graph-container");
                const root = state.nodes.find(n => n.id === entId);
                if (root) { root.x = container.clientWidth / 2; root.y = container.clientHeight / 2; root.fx = root.x; root.fy = root.y; }
            }

            for (const p of data.parties) {
                if (!p.party_name) continue;
                const pid = addNode("person", p.party_name, p.party_name, p);
                addLink(entId, pid, p.party_role || "Officer");
            }

            for (const a of data.addresses) {
                const aid = addNode("property", a.folio, a.address || a.folio, a);
                addLink(entId, aid, a.match_type === "principal_address" ? "Principal Address" : "Mailing Address");
            }

            document.getElementById("graph-toolbar").style.display = "flex";
            updateGraph();
            updateStatus(data.parties.length + " people, " + data.addresses.length + " addresses");
        } catch (err) {
            updateStatus("Error loading entity");
            console.error(err);
        }
    }

    async function loadPerson(name) {
        if (state.expandedIds.has("person:" + name)) return;
        state.expandedIds.add("person:" + name);
        updateStatus("Loading person...");

        try {
            const resp = await fetch("/api/connections/person?name=" + encodeURIComponent(name));
            const data = await resp.json();

            const pid = nodeId("person", name);

            for (const e of data.entities) {
                const eid = addNode("entity", e.doc_number, e.entity_name, e);
                addLink(pid, eid, e.party_role || "Officer");
            }

            for (const p of data.properties) {
                const propId = addNode("property", p.folio, p.address || p.folio, p);
                addLink(pid, propId, "Owner");
            }

            updateGraph();
            updateStatus(data.entities.length + " entities, " + data.properties.length + " properties");
        } catch (err) {
            updateStatus("Error loading person");
            console.error(err);
        }
    }

    async function loadProperty(folio) {
        if (state.expandedIds.has("property:" + folio)) return;
        state.expandedIds.add("property:" + folio);
        updateStatus("Loading property...");

        try {
            const resp = await fetch("/api/connections/property/" + encodeURIComponent(folio));
            const data = await resp.json();

            const propId = nodeId("property", folio);

            for (const o of data.owners) {
                if (!o.name) continue;
                const pid = addNode("person", o.name, o.name, { party_role: o.role, party_title: "" });
                addLink(propId, pid, o.role || "Owner");
            }

            for (const e of data.registered_entities) {
                const eid = addNode("entity", e.doc_number, e.entity_name, e);
                addLink(propId, eid, "Registered Address");
            }

            updateGraph();
            updateStatus(data.owners.length + " owners, " + data.registered_entities.length + " registered entities");
        } catch (err) {
            updateStatus("Error loading property");
            console.error(err);
        }
    }

    // ---- Render ----
    function updateGraph() {
        const g = state.g;

        // Links
        const linkSel = g.select(".links-group").selectAll(".edge-group")
            .data(state.links, d => (d.source.id || d.source) + "-" + (d.target.id || d.target));

        const linkEnter = linkSel.enter().append("g").attr("class", "edge-group");
        linkEnter.append("line").attr("class", "edge-line");
        linkEnter.append("text").attr("class", "edge-label").text(d => d.label);
        linkSel.exit().remove();

        // Nodes
        const nodeSel = g.select(".nodes-group").selectAll(".node-group")
            .data(state.nodes, d => d.id);

        const nodeEnter = nodeSel.enter().append("g")
            .attr("class", d => "node-group node-" + d.type)
            .call(d3.drag()
                .on("start", dragStarted)
                .on("drag", dragged)
                .on("end", dragEnded))
            .on("click", onNodeClick)
            .on("mouseover", (event, d) => showTooltip(event, d))
            .on("mousemove", (event) => positionTooltip(event))
            .on("mouseout", hideTooltip);

        // Entity: rectangle
        nodeEnter.filter(d => d.type === "entity")
            .append("rect")
            .attr("width", d => Math.max(d.label.length * 6, NODE_SIZE.entity * 2))
            .attr("height", NODE_SIZE.entity)
            .attr("x", d => -Math.max(d.label.length * 6, NODE_SIZE.entity * 2) / 2)
            .attr("y", -NODE_SIZE.entity / 2);

        // Person: circle
        nodeEnter.filter(d => d.type === "person")
            .append("circle").attr("r", NODE_SIZE.person);

        // Property: pentagon
        nodeEnter.filter(d => d.type === "property")
            .append("polygon").attr("points", pentagonPoints(NODE_SIZE.property));

        // Labels
        nodeEnter.append("text")
            .attr("class", "node-label")
            .attr("dy", d => d.type === "entity" ? NODE_SIZE.entity + 10 : (d.type === "person" ? NODE_SIZE.person + 12 : NODE_SIZE.property + 14))
            .text(d => truncate(d.label, 20));

        nodeSel.exit().remove();

        // Force simulation
        if (state.simulation) state.simulation.stop();
        const container = document.querySelector(".connections-graph-container");
        state.simulation = d3.forceSimulation(state.nodes)
            .force("link", d3.forceLink(state.links).id(d => d.id).distance(120))
            .force("charge", d3.forceManyBody().strength(-300))
            .force("center", d3.forceCenter(container.clientWidth / 2, container.clientHeight / 2))
            .force("collision", d3.forceCollide().radius(40))
            .on("tick", ticked);

        state.simulation.alpha(0.5).restart();
    }

    function ticked() {
        state.g.select(".links-group").selectAll(".edge-group").each(function (d) {
            const g = d3.select(this);
            g.select("line")
                .attr("x1", d.source.x).attr("y1", d.source.y)
                .attr("x2", d.target.x).attr("y2", d.target.y);
            g.select("text")
                .attr("x", (d.source.x + d.target.x) / 2)
                .attr("y", (d.source.y + d.target.y) / 2 - 4);
        });

        state.g.select(".nodes-group").selectAll(".node-group")
            .attr("transform", d => `translate(${d.x},${d.y})`);
    }

    function onNodeClick(event, d) {
        event.stopPropagation();
        if (d.type === "entity") loadEntity(d.key, false);
        else if (d.type === "person") loadPerson(d.key);
        else if (d.type === "property") loadProperty(d.key);
    }

    // ---- Drag ----
    function dragStarted(event, d) {
        if (!event.active) state.simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
    }
    function dragged(event, d) { d.fx = event.x; d.fy = event.y; }
    function dragEnded(event, d) {
        if (!event.active) state.simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
    }

    // ---- Reset ----
    function resetGraph() {
        state.nodes.length = 0;
        state.links.length = 0;
        state.expandedIds.clear();
        if (state.simulation) state.simulation.stop();
        state.g.select(".links-group").selectAll("*").remove();
        state.g.select(".nodes-group").selectAll("*").remove();
        if (state.rootEntity) loadEntity(state.rootEntity, true);
    }

    // ---- Helpers ----
    function pentagonPoints(r) {
        const pts = [];
        for (let i = 0; i < 5; i++) {
            const angle = (Math.PI * 2 * i / 5) - Math.PI / 2;
            pts.push(Math.cos(angle) * r + "," + Math.sin(angle) * r);
        }
        return pts.join(" ");
    }

    function truncate(s, n) { return s && s.length > n ? s.substring(0, n) + "…" : s; }
    function esc(s) { const d = document.createElement("div"); d.textContent = s || ""; return d.innerHTML; }
    function updateStatus(msg) { document.getElementById("graph-status").textContent = msg; }

    // ---- Boot ----
    document.addEventListener("DOMContentLoaded", init);
})();
