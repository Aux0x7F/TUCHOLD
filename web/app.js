(function () {
  const raw = window.TUCHOLD_FLOW_DATA;
  const rawNodeById = Object.fromEntries(raw.nodes.map((node) => [node.id, node]));
  const rawFacilities = raw.nodes.filter((node) => node.kind === "facility");
  const stayById = Object.fromEntries(raw.stays.map((stay) => [stay.id, stay]));
  const stateNames = { AZ: "Arizona", CA: "California", CU: "Cuba", FL: "Florida", LA: "Louisiana", MS: "Mississippi", NM: "New Mexico", TX: "Texas", WA: "Washington" };
  const pinnedNodeIds = new Set(["TUCHOLD"]);
  const cityNameCounts = rawFacilities.reduce((counts, node) => ((counts[node.city] = (counts[node.city] || 0) + 1), counts), {});
  const defaultFilter = {
    start: raw.metadata.minStartDate || "",
    end: raw.metadata.maxStartDate || "",
  };
  const groupedNodes = { state: buildNodes("state"), city: buildNodes("city"), facility: buildNodes("facility") };
  const baseGraphs = {};
  const homeBounds = L.latLngBounds(rawFacilities.map((node) => [node.lat, node.lon]));
  const number = new Intl.NumberFormat("en-US");
  const app = {
    level: null,
    node: null,
    corridor: null,
    open: new Set(),
    frame: 0,
    pendingFull: false,
    interacting: false,
    startDate: defaultFilter.start,
    endDate: defaultFilter.end,
    filterKey: "",
    filteredStays: raw.stays,
  };

  const els = {
    years: document.getElementById("year-summary"),
    title: document.getElementById("detail-title"),
    metrics: document.getElementById("detail-metrics"),
    sections: document.getElementById("detail-sections"),
    list: document.getElementById("connection-list"),
    caption: document.getElementById("connection-caption"),
    reset: document.getElementById("reset-selection"),
    filterStart: document.getElementById("filter-start"),
    filterEnd: document.getElementById("filter-end"),
  };

  const map = L.map("map", { worldCopyJump: true, minZoom: 2 }).setView([23, -35], 2);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    maxZoom: 19,
  }).addTo(map);

  const layers = {
    lines: L.layerGroup().addTo(map),
    nodes: L.layerGroup().addTo(map),
  };

  function fmt(value) {
    return number.format(value);
  }

  function pct(value, total) {
    return `${(((value || 0) / (total || 1)) * 100).toFixed(1)}%`;
  }

  function share(value) {
    return `${((value || 0) * 100).toFixed(1)}%`;
  }

  function hours(value) {
    return Number.isFinite(value) ? `${value.toFixed(1)}h` : "n/a";
  }

  function mean(values) {
    return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  }

  function median(values) {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function bucketSentence(days) {
    if (!Number.isFinite(days)) return "No sentence";
    if (days < 30) return "<30d";
    if (days < 180) return "30-179d";
    if (days < 365) return "180-364d";
    if (days < 365 * 5) return "1-5y";
    if (days < 365 * 10) return "5-10y";
    return ">10y";
  }

  function tally(stays, accessor, limit = 8) {
    const counts = new Map();
    stays.forEach((stay) => {
      const key = accessor(stay) || "Unspecified";
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    return [...counts.entries()]
      .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
      .slice(0, limit)
      .map(([label, count]) => ({ label, count, share: count / (stays.length || 1) }));
  }

  function summarize(stays) {
    const hold = stays.map((stay) => stay.holdHours).filter(Number.isFinite);
    const total = stays.map((stay) => stay.totalHours).filter(Number.isFinite);
    return {
      stats: {
        stays: stays.length,
        countryExits: stays.filter((stay) => stay.path.at(-1)?.startsWith("COUNTRY:")).length,
        holdMedian: median(hold),
        holdMean: mean(hold),
        totalMedian: median(total),
        totalMean: mean(total),
        over72: stays.filter((stay) => stay.over72).length,
      },
      sections: [
        { title: "Final charges", rows: tally(stays, (stay) => stay.finalCharge) },
        { title: "MSC charges", rows: tally(stays, (stay) => stay.mscCharge) },
        { title: "Sentencing", rows: tally(stays, (stay) => bucketSentence(stay.sentenceDays), 6) },
        { title: "Release", rows: tally(stays, (stay) => stay.releaseReason) },
      ],
    };
  }

  function normalizeDateRange(start, end) {
    let valueStart = start || "";
    let valueEnd = end || "";
    if (valueStart && valueEnd && valueStart > valueEnd) [valueStart, valueEnd] = [valueEnd, valueStart];
    return { start: valueStart, end: valueEnd };
  }

  function setFilterInputs(start, end) {
    els.filterStart.value = start || "";
    els.filterEnd.value = end || "";
  }

  function filteredStays() {
    const range = normalizeDateRange(app.startDate, app.endDate);
    const key = `${range.start}|${range.end}`;
    if (app.filterKey === key) return app.filteredStays;
    app.filterKey = key;
    app.startDate = range.start;
    app.endDate = range.end;
    setFilterInputs(range.start, range.end);
    app.filteredStays = raw.stays.filter((stay) => {
      if (range.start && stay.startDate < range.start) return false;
      if (range.end && stay.startDate > range.end) return false;
      return true;
    });
    return app.filteredStays;
  }

  function yearCounts(stays) {
    const counts = Object.fromEntries((raw.metadata.years || []).map((year) => [String(year), 0]));
    stays.forEach((stay) => {
      const year = stay.startDate ? stay.startDate.slice(0, 4) : null;
      if (year && year in counts) counts[year] += 1;
    });
    return counts;
  }

  function cityKey(state, city) {
    return state && city ? `${state}|${city}` : null;
  }

  function parseCityKey(value) {
    const [state, ...rest] = String(value || "").split("|");
    return { state, city: rest.join("|") };
  }

  function cityLabel(city, state) {
    return cityNameCounts[city] > 1 ? `${city}, ${state}` : city;
  }

  function groupId(node, level) {
    if (node.kind === "country") return node.id;
    if (level === "facility") return node.id;
    if (level === "city") return `CITY:${node.state}:${node.city}`;
    return `STATE:${node.state}`;
  }

  function groupLabel(node, level) {
    if (node.kind === "country") return node.label;
    if (level === "facility") return node.label;
    if (level === "city") return cityLabel(node.city, node.state);
    return stateNames[node.state] || node.state;
  }

  function buildNodes(level) {
    const grouped = new Map();
    raw.nodes.forEach((node) => {
      const key = groupId(node, level);
      const point = { lat: node.lat, lon: node.lon };
      if (!grouped.has(key)) {
        grouped.set(key, {
          id: key,
          kind: node.kind === "country" ? "country" : level,
          label: groupLabel(node, level),
          latSum: 0,
          lonSum: 0,
          count: 0,
          rawMembers: [],
          stateKey: node.kind === "country" ? null : node.state,
          cityKey: node.kind === "facility" || level === "city" ? cityKey(node.state, node.city) : null,
        });
      }
      const entry = grouped.get(key);
      entry.latSum += point.lat;
      entry.lonSum += point.lon;
      entry.count += 1;
      entry.rawMembers.push(node.id);
    });
    return Object.fromEntries(
      [...grouped.values()].map((entry) => [
        entry.id,
        {
          id: entry.id,
          kind: entry.kind,
          label: entry.label,
          lat: entry.latSum / entry.count,
          lon: entry.lonSum / entry.count,
          members: entry.rawMembers,
          stateKey: entry.stateKey,
          cityKey: entry.cityKey,
          moveCount: 0,
          stayCount: 0,
          stayIds: [],
          progressSum: 0,
          progressCount: 0,
          progressMean: 0,
          summary: summarize([]),
        },
      ])
    );
  }

  function graphFor(level, stays, cacheKey) {
    if (baseGraphs[level]?.key === cacheKey) return baseGraphs[level].graph;
    const graphNodes = Object.fromEntries(
      Object.entries(groupedNodes[level]).map(([id, node]) => [id, { ...node, stayIds: new Set() }])
    );
    const corridors = new Map();

    stays.forEach((stay) => {
      const legs = Math.max(stay.path.length - 1, 0);

      stay.path.forEach((nodeId, index) => {
        const groupedId = groupId(rawNodeById[nodeId], level);
        if (!groupedId || !graphNodes[groupedId]) return;
        graphNodes[groupedId].stayIds.add(stay.id);
        graphNodes[groupedId].progressSum += legs ? index / legs : 0;
        graphNodes[groupedId].progressCount += 1;
      });

      for (let index = 0; index < stay.path.length - 1; index += 1) {
        const fromId = groupId(rawNodeById[stay.path[index]], level);
        const toId = groupId(rawNodeById[stay.path[index + 1]], level);
        if (!fromId || !toId || fromId === toId) continue;

        const a = fromId < toId ? fromId : toId;
        const b = fromId < toId ? toId : fromId;
        const key = `${a}__${b}`;
        if (!corridors.has(key)) {
          corridors.set(key, {
            key,
            a,
            b,
            label: `${graphNodes[a].label} - ${graphNodes[b].label}`,
            total: 0,
            forward: 0,
            backward: 0,
            progressSum: 0,
            progressCount: 0,
            stayIds: new Set(),
            segments: [],
          });
        }

        const corridor = corridors.get(key);
        const legRatio = legs ? (index + 1) / legs : 0;
        corridor.total += 1;
        if (fromId === a) corridor.forward += 1;
        else corridor.backward += 1;
        corridor.progressSum += legRatio;
        corridor.progressCount += 1;
        corridor.stayIds.add(stay.id);
        corridor.segments.push({ stayId: stay.id, fromId, toId, legRatio });
        graphNodes[fromId].moveCount += 1;
        graphNodes[toId].moveCount += 1;
      }
    });

    Object.values(graphNodes).forEach((node) => {
      node.stayIds = [...node.stayIds];
      node.stayCount = node.stayIds.length;
      node.progressMean = node.progressCount ? node.progressSum / node.progressCount : 0;
      node.summary = summarize(node.stayIds.map((id) => stayById[id]));
    });

    const corridorList = [...corridors.values()]
      .map((corridor) => {
        const stayIds = [...corridor.stayIds];
        return {
          ...corridor,
          stayIds,
          progressMean: corridor.progressCount ? corridor.progressSum / corridor.progressCount : 0,
          summary: summarize(stayIds.map((id) => stayById[id])),
        };
      })
      .sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));

    const graph = {
      level,
      nodes: graphNodes,
      corridors: corridorList,
      corridorMap: Object.fromEntries(corridorList.map((corridor) => [corridor.key, corridor])),
      maxTotal: Math.max(...corridorList.map((corridor) => corridor.total), 1),
    };
    baseGraphs[level] = { key: cacheKey, graph };
    return graph;
  }

  function screenThreshold() {
    const size = map.getSize();
    return Math.max(56, Math.min(size.x, size.y) * 0.1);
  }

  function pointDistance(a, b) {
    const start = map.latLngToLayerPoint([a.lat, a.lon]);
    const end = map.latLngToLayerPoint([b.lat, b.lon]);
    return start.distanceTo(end);
  }

  function clusterLabel(baseNodes, stateKey, cityValue) {
    if (baseNodes.length === 1) return baseNodes[0].label;
    if (baseNodes.every((node) => node.kind === "country")) return `${baseNodes.length} countries`;
    if (cityValue) {
      const { state, city } = parseCityKey(cityValue);
      return cityLabel(city, state);
    }
    if (stateKey) return stateNames[stateKey] || stateKey;
    return `${baseNodes[0].label} + ${baseNodes.length - 1}`;
  }

  function clusterFromMembers(memberIds, baseNodes) {
    const members = [...new Set(memberIds)].sort();
    const parts = members.map((id) => baseNodes[id]);
    const stateKeys = [...new Set(parts.map((part) => part.stateKey).filter(Boolean))];
    const cityKeys = [...new Set(parts.map((part) => part.cityKey).filter(Boolean))];
    const weight = parts.reduce((sum, part) => sum + Math.max(1, part.members.length), 0);
    const stateKey = stateKeys.length === 1 ? stateKeys[0] : null;
    const cityValue = cityKeys.length === 1 ? cityKeys[0] : null;

    return {
      id: `NODE:${members.join("|")}`,
      members,
      lat: parts.reduce((sum, part) => sum + part.lat * Math.max(1, part.members.length), 0) / weight,
      lon: parts.reduce((sum, part) => sum + part.lon * Math.max(1, part.members.length), 0) / weight,
      stateKey,
      cityKey: cityValue,
      kind: parts.every((part) => part.kind === "country") ? "country" : cityValue ? "city" : stateKey ? "state" : "cluster",
      label: clusterLabel(parts, stateKey, cityValue),
    };
  }

  function mergeNearby(groups, keyFn, baseNodes) {
    const parent = Object.fromEntries(groups.map((group) => [group.id, group.id]));
    const isPinned = (group) => group.members.some((id) => pinnedNodeIds.has(id));
    const find = (id) => {
      if (parent[id] !== id) parent[id] = find(parent[id]);
      return parent[id];
    };
    const join = (left, right) => {
      const a = find(left);
      const b = find(right);
      if (a !== b) parent[b] = a;
    };

    const buckets = new Map();
    groups.forEach((group) => {
      const key = keyFn(group);
      if (!key) return;
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(group);
    });

    const threshold = screenThreshold();
    buckets.forEach((items) => {
      for (let index = 0; index < items.length; index += 1) {
        for (let next = index + 1; next < items.length; next += 1) {
          if (isPinned(items[index]) || isPinned(items[next])) continue;
          if (pointDistance(items[index], items[next]) <= threshold) join(items[index].id, items[next].id);
        }
      }
    });

    const merged = new Map();
    groups.forEach((group) => {
      const root = find(group.id);
      if (!merged.has(root)) merged.set(root, []);
      merged.get(root).push(...group.members);
    });

    return [...merged.values()].map((memberIds) => clusterFromMembers(memberIds, baseNodes));
  }

  function collapseNodes(baseGraph) {
    let groups = Object.values(baseGraph.nodes).map((node) => clusterFromMembers([node.id], baseGraph.nodes));
    if (baseGraph.level === "facility") {
      groups = mergeNearby(groups, (group) => group.cityKey, baseGraph.nodes);
      groups = mergeNearby(groups, (group) => group.stateKey, baseGraph.nodes);
    } else if (baseGraph.level === "city") {
      groups = mergeNearby(groups, (group) => group.stateKey, baseGraph.nodes);
    }
    return groups;
  }

  function viewGraphFor(baseGraph) {
    const clusters = collapseNodes(baseGraph);
    const nodeByBase = {};
    const viewNodes = {};

    clusters.forEach((cluster) => {
      cluster.members.forEach((id) => {
        nodeByBase[id] = cluster.id;
      });
      const stayIds = [...new Set(cluster.members.flatMap((id) => baseGraph.nodes[id].stayIds))];
      const progressSum = cluster.members.reduce((sum, id) => sum + baseGraph.nodes[id].progressSum, 0);
      const progressCount = cluster.members.reduce((sum, id) => sum + baseGraph.nodes[id].progressCount, 0);
      viewNodes[cluster.id] = {
        ...cluster,
        stayIds,
        stayCount: stayIds.length,
        moveCount: 0,
        progressSum,
        progressCount,
        progressMean: progressCount ? progressSum / progressCount : 0,
        summary: summarize(stayIds.map((id) => stayById[id])),
      };
    });

    const corridors = new Map();
    baseGraph.corridors.forEach((baseCorridor) => {
      baseCorridor.segments.forEach((segment) => {
        const fromNode = nodeByBase[segment.fromId];
        const toNode = nodeByBase[segment.toId];
        if (!fromNode || !toNode || fromNode === toNode) return;

        const a = fromNode < toNode ? fromNode : toNode;
        const b = fromNode < toNode ? toNode : fromNode;
        const key = `${a}__${b}`;
        if (!corridors.has(key)) {
          corridors.set(key, {
            key,
            a,
            b,
            label: `${viewNodes[a].label} - ${viewNodes[b].label}`,
            total: 0,
            forward: 0,
            backward: 0,
            progressSum: 0,
            progressCount: 0,
            stayIds: new Set(),
            segments: [],
          });
        }

        const corridor = corridors.get(key);
        corridor.total += 1;
        if (fromNode === a) corridor.forward += 1;
        else corridor.backward += 1;
        corridor.progressSum += segment.legRatio;
        corridor.progressCount += 1;
        corridor.stayIds.add(segment.stayId);
        corridor.segments.push(segment);
      });
    });

    const corridorList = [...corridors.values()]
      .map((corridor) => {
        const stayIds = [...corridor.stayIds];
        return {
          ...corridor,
          stayIds,
          progressMean: corridor.progressCount ? corridor.progressSum / corridor.progressCount : 0,
          summary: summarize(stayIds.map((id) => stayById[id])),
        };
      })
      .sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));

    corridorList.forEach((corridor) => {
      viewNodes[corridor.a].moveCount += corridor.total;
      viewNodes[corridor.b].moveCount += corridor.total;
    });

    return {
      level: baseGraph.level,
      base: baseGraph,
      nodes: viewNodes,
      corridors: corridorList,
      corridorMap: Object.fromEntries(corridorList.map((corridor) => [corridor.key, corridor])),
      maxTotal: Math.max(...corridorList.map((corridor) => corridor.total), 1),
    };
  }

  function memberRoutes(corridor, graph) {
    const routes = new Map();
    corridor.segments.forEach((segment) => {
      const from = graph.base.nodes[segment.fromId];
      const to = graph.base.nodes[segment.toId];
      if (!from || !to || from.id === to.id) return;
      const a = from.id < to.id ? from : to;
      const b = from.id < to.id ? to : from;
      const key = `${a.id}__${b.id}`;
      if (!routes.has(key)) routes.set(key, { key, label: `${a.label} - ${b.label}`, total: 0, stayIds: new Set() });
      const route = routes.get(key);
      route.total += 1;
      route.stayIds.add(segment.stayId);
    });

    return [...routes.values()]
      .map((route) => {
        const stayIds = [...route.stayIds];
        return { ...route, stayIds, summary: summarize(stayIds.map((id) => stayById[id])) };
      })
      .sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));
  }

  function currentLevel() {
    const bounds = map.getBounds().pad(0.05);
    const visibleCountries = raw.nodes.filter((node) => node.kind === "country" && bounds.contains([node.lat, node.lon])).length;
    const visibleFacilities = rawFacilities.filter((node) => bounds.contains([node.lat, node.lon]));
    const visibleStates = new Set(visibleFacilities.map((node) => node.state)).size;
    const visibleCities = new Set(visibleFacilities.map((node) => cityKey(node.state, node.city))).size;
    const zoom = map.getZoom();

    if (visibleCountries > 1 || zoom <= 3) return "state";
    if (zoom >= 7 && visibleStates <= 2 && visibleCities <= 10) return "facility";
    return "city";
  }
  function metricCards(summary, first, second) {
    const { stats } = summary;
    return [
      [first.label, first.value],
      [second.label, second.value],
      ["med hold hr", hours(stats.holdMedian)],
      ["avg hold hr", hours(stats.holdMean)],
      ["med total hr", hours(stats.totalMedian)],
      [">72hr", `${fmt(stats.over72)} | ${pct(stats.over72, stats.stays)}`],
    ]
      .map(([label, value]) => `<article class="card"><strong>${value}</strong><span>${label}</span></article>`)
      .join("");
  }

  function sectionRows(rows) {
    return rows.map((row) => `<li><span>${row.label}</span><span>${fmt(row.count)} | ${share(row.share)}</span></li>`).join("");
  }

  function sectionBlocks(sections) {
    return sections
      .filter((section) => section.rows.length)
      .map(
        (section) => `
          <section class="block">
            <header><h3>${section.title}</h3></header>
            <ul>${sectionRows(section.rows)}</ul>
          </section>
        `
      )
      .join("");
  }

  function locationRows(node, graph) {
    const members = node.members
      .map((id) => graph.base.nodes[id])
      .sort((a, b) => b.stayCount - a.stayCount || a.label.localeCompare(b.label))
      .slice(0, 8);
    const total = members.reduce((sum, member) => sum + member.stayCount, 0) || 1;
    return members.map((member) => ({ label: member.label, count: member.stayCount, share: member.stayCount / total }));
  }

  function connectionRows(node, graph) {
    return graph.corridors
      .filter((corridor) => corridor.a === node.id || corridor.b === node.id)
      .map((corridor) => {
        const other = graph.nodes[corridor.a === node.id ? corridor.b : corridor.a];
        return { label: other.label, count: corridor.total, share: corridor.total / (node.moveCount || 1) };
      })
      .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
      .slice(0, 8);
  }

  function setNode(graph, key) {
    app.node = key;
    app.corridor = null;
    app.open = new Set();
    render(true);
  }

  function setCorridor(graph, key) {
    app.corridor = key;
    app.node = null;
    const routes = graph.corridorMap[key] ? memberRoutes(graph.corridorMap[key], graph) : [];
    app.open = new Set(routes.length === 1 ? [routes[0].key] : []);
    render(true);
  }

  function renderYears(stays) {
    els.years.innerHTML = Object.entries(yearCounts(stays))
      .map(([year, count]) => `<span class="chip">${year}: ${fmt(count)}</span>`)
      .concat(`<span class="chip">Total: ${fmt(stays.length)}</span>`)
      .join("");
  }

  function renderList(graph) {
    els.caption.textContent = `${graph.level} view | ${fmt(graph.corridors.length)} shown`;
    els.list.innerHTML = graph.corridors
      .slice(0, 20)
      .map(
        (corridor) => `
          <button class="item${corridor.key === app.corridor ? " active" : ""}" type="button" data-key="${corridor.key}">
            <span>${corridor.label}</span>
            <strong>${fmt(corridor.total)}</strong>
          </button>
        `
      )
      .join("");

    els.list.querySelectorAll("[data-key]").forEach((button) => {
      button.addEventListener("click", () => setCorridor(graph, button.dataset.key));
    });
  }

  function renderOverview(summary) {
    els.title.textContent = "Overview";
    els.metrics.innerHTML = metricCards(
      summary,
      { label: "stays", value: fmt(summary.stats.stays) },
      { label: "exits", value: fmt(summary.stats.countryExits) }
    );
    els.sections.innerHTML = sectionBlocks(summary.sections);
  }

  function renderNodeDetail(node, graph) {
    const sections = [];
    if (node.members.length > 1) sections.push({ title: "Locations", rows: locationRows(node, graph) });
    const links = connectionRows(node, graph);
    if (links.length) sections.push({ title: "Connections", rows: links });
    sections.push(...node.summary.sections);

    els.title.textContent = node.label;
    els.metrics.innerHTML = metricCards(
      node.summary,
      { label: "stays", value: fmt(node.summary.stats.stays) },
      { label: "moves", value: fmt(node.moveCount) }
    );
    els.sections.innerHTML = sectionBlocks(sections);
  }

  function renderCorridorDetail(corridor, graph) {
    const routes = memberRoutes(corridor, graph);
    els.title.textContent = corridor.label;
    els.metrics.innerHTML = metricCards(
      corridor.summary,
      { label: "moves", value: fmt(corridor.total) },
      { label: "stays", value: fmt(corridor.summary.stats.stays) }
    );
    els.sections.innerHTML = routes
      .map((route) => {
        const open = app.open.has(route.key);
        return `
          <section class="tile${open ? " open" : ""}">
            <button class="tile-head" type="button" data-route="${route.key}">
              <span>${route.label}</span>
              <strong>${fmt(route.total)}</strong>
            </button>
            ${
              open
                ? `<div class="tile-body">
                    <div class="mini-grid">${metricCards(
                      route.summary,
                      { label: "moves", value: fmt(route.total) },
                      { label: "stays", value: fmt(route.summary.stats.stays) }
                    )}</div>
                    ${sectionBlocks(route.summary.sections)}
                  </div>`
                : ""
            }
          </section>
        `;
      })
      .join("");

    els.sections.querySelectorAll("[data-route]").forEach((button) => {
      button.addEventListener("click", () => {
        if (app.open.has(button.dataset.route)) app.open.delete(button.dataset.route);
        else app.open.add(button.dataset.route);
        renderCorridorDetail(corridor, graph);
      });
    });
  }

  function renderDetail(graph, summary) {
    if (app.corridor && graph.corridorMap[app.corridor]) {
      renderCorridorDetail(graph.corridorMap[app.corridor], graph);
      return;
    }
    if (app.node && graph.nodes[app.node]) {
      renderNodeDetail(graph.nodes[app.node], graph);
      return;
    }
    renderOverview(summary);
  }
  function lineWeight(total, maxTotal, active) {
    if (total < 2) return active ? 2.25 : 1;
    if (total < 5) return active ? 3 : 1.8;
    const floor = Math.log(5);
    const ceiling = Math.max(floor + 0.0001, Math.log(Math.max(5, maxTotal)));
    const ratio = (Math.log(total) - floor) / (ceiling - floor);
    const scaled = 2.4 + Math.max(0, Math.min(1, ratio)) * 6.6;
    return active ? scaled + 1.35 : scaled;
  }

  function lineDash(total) {
    if (total < 2) return "1 10";
    if (total < 5) return "8 8";
    return null;
  }

  function lineCap(total) {
    return total < 2 ? "round" : "butt";
  }

  function mixColor(start, end, ratio) {
    const clamped = Math.max(0, Math.min(1, ratio));
    const value = start.map((channel, index) => Math.round(channel + (end[index] - channel) * clamped));
    return `rgb(${value[0]}, ${value[1]}, ${value[2]})`;
  }

  function progressColor(progress) {
    return mixColor([216, 128, 45], [115, 77, 172], progress);
  }

  function strokeColor(progress) {
    return mixColor([166, 93, 28], [78, 48, 130], progress);
  }

  function corridorColor(corridor, graph) {
    return progressColor(corridor.progressMean);
  }

  function hitWeight(weight) {
    return Math.max(14, weight + 10);
  }

  function nodeVolume(node) {
    return node.moveCount + node.stayCount;
  }

  function nodeRadius(volume, maxVolume, active) {
    if (maxVolume <= 1) return active ? 4.5 : 3.25;
    const ratio = Math.log(volume + 1) / Math.log(maxVolume + 1);
    const scaled = 3.25 + Math.max(0, Math.min(1, ratio)) * 11.75;
    return active ? scaled + 1 : scaled;
  }

  function corridorTooltip(corridor, graph) {
    const from = graph.nodes[corridor.a];
    const to = graph.nodes[corridor.b];
    const lines = [];
    if (corridor.forward) lines.push(`${from.label} -> ${to.label} ${fmt(corridor.forward)}`);
    if (corridor.backward) lines.push(`${to.label} -> ${from.label} ${fmt(corridor.backward)}`);
    return lines.join("<br>");
  }

  function renderMap(graph) {
    layers.lines.clearLayers();
    layers.nodes.clearLayers();
    const maxNodeVolume = Math.max(...Object.values(graph.nodes).map((node) => nodeVolume(node)), 1);

    [...graph.corridors]
      .sort((left, right) => {
        const leftActive = left.key === app.corridor ? 1 : 0;
        const rightActive = right.key === app.corridor ? 1 : 0;
        return leftActive - rightActive || left.total - right.total || left.label.localeCompare(right.label);
      })
      .forEach((corridor) => {
      const from = graph.nodes[corridor.a];
      const to = graph.nodes[corridor.b];
      const active = corridor.key === app.corridor;
      const weight = lineWeight(corridor.total, graph.maxTotal, active);
      const color = corridorColor(corridor, graph);
      const dashArray = lineDash(corridor.total);
      const cap = lineCap(corridor.total);
      const path = [
        [from.lat, from.lon],
        [to.lat, to.lon],
      ];

      L.polyline(
        path,
        {
          color,
          weight,
          opacity: active ? 0.92 : 0.58,
          lineCap: cap,
          lineJoin: "round",
          dashArray,
          interactive: false,
        }
      ).addTo(layers.lines);

      L.polyline(path, {
        color: "#000",
        weight: hitWeight(weight),
        opacity: 0.001,
        lineCap: "round",
        lineJoin: "round",
        className: "flow-hit",
      })
        .bindTooltip(corridorTooltip(corridor, graph), {
          className: "flow-tip",
          direction: "auto",
          sticky: true,
          opacity: 1,
        })
        .on("click", () => setCorridor(graph, corridor.key))
        .addTo(layers.lines);
      });

    Object.values(graph.nodes).forEach((node) => {
      if (!node.moveCount && !node.stayCount) return;
      const corridor = graph.corridorMap[app.corridor];
      const active = node.id === app.node || (corridor && [corridor.a, corridor.b].includes(node.id));
      const fill = progressColor(node.progressMean);
      const stroke = active ? strokeColor(node.progressMean) : fill;
      const radius = nodeRadius(nodeVolume(node), maxNodeVolume, active);
      L.circleMarker([node.lat, node.lon], {
        radius,
        weight: active ? 2 : 1,
        color: stroke,
        fillColor: fill,
        fillOpacity: 0.85,
      })
        .on("click", () => setNode(graph, node.id))
        .addTo(layers.nodes);
    });
  }

  function syncGraph() {
    const stays = filteredStays();
    const level = currentLevel();
    const levelChanged = app.level !== level;
    app.level = level;
    const cacheKey = `${level}|${app.filterKey}`;
    const graph = viewGraphFor(graphFor(level, stays, cacheKey));
    const summary = summarize(stays);
    let selectionChanged = false;

    if (app.node && !graph.nodes[app.node]) {
      app.node = null;
      selectionChanged = true;
    }
    if (app.corridor && !graph.corridorMap[app.corridor]) {
      app.corridor = null;
      app.open = new Set();
      selectionChanged = true;
    }

    return { stays, graph, summary, levelChanged, selectionChanged };
  }

  function render(full = true) {
    const { stays, graph, summary, levelChanged, selectionChanged } = syncGraph();
    if (full || levelChanged || selectionChanged) {
      renderYears(stays);
      renderList(graph);
      renderDetail(graph, summary);
    }
    renderMap(graph);
  }

  function scheduleRender(full = false) {
    if (full) app.pendingFull = true;
    if (app.interacting) return;
    if (app.frame) return;
    app.frame = window.requestAnimationFrame(() => {
      app.frame = 0;
      render(app.pendingFull);
      app.pendingFull = false;
    });
  }

  function cancelScheduledRender() {
    if (!app.frame) return;
    window.cancelAnimationFrame(app.frame);
    app.frame = 0;
  }

  function applyFilter(start, end) {
    const range = normalizeDateRange(start, end);
    app.startDate = range.start;
    app.endDate = range.end;
    app.filterKey = "";
    app.node = null;
    app.corridor = null;
    app.open = new Set();
    render(true);
  }

  els.reset.addEventListener("click", () => {
    applyFilter(defaultFilter.start, defaultFilter.end);
    if (homeBounds.isValid()) map.fitBounds(homeBounds, { padding: [60, 60], maxZoom: 6 });
  });

  els.filterStart.min = raw.metadata.minStartDate || "";
  els.filterStart.max = raw.metadata.maxStartDate || "";
  els.filterEnd.min = raw.metadata.minStartDate || "";
  els.filterEnd.max = raw.metadata.maxStartDate || "";
  setFilterInputs(defaultFilter.start, defaultFilter.end);

  els.filterStart.addEventListener("change", () => applyFilter(els.filterStart.value, els.filterEnd.value));
  els.filterEnd.addEventListener("change", () => applyFilter(els.filterStart.value, els.filterEnd.value));

  map.on("zoomstart movestart", () => {
    app.interacting = true;
    cancelScheduledRender();
  });
  map.on("zoomend moveend", () => {
    app.interacting = false;
    scheduleRender(true);
  });
  map.on("resize", () => scheduleRender(true));
  if (homeBounds.isValid()) map.fitBounds(homeBounds, { padding: [60, 60], maxZoom: 6 });
  render(true);
})();
