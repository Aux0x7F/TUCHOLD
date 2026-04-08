(function () {
  const raw = window.TUCHOLD_FLOW_DATA;
  const number = new Intl.NumberFormat("en-US");
  const rawNodeById = indexBy(raw.nodes, "id");
  const stayById = indexBy(raw.stays, "id");
  const facilities = raw.nodes.filter(({ kind }) => kind === "facility");
  const cityCounts = facilities.reduce((counts, { city }) => ((counts[city] = (counts[city] || 0) + 1), counts), {});
  const pinnedNodes = new Set(["TUCHOLD"]);
  const stateNames = { AZ: "Arizona", CA: "California", CU: "Cuba", FL: "Florida", LA: "Louisiana", MS: "Mississippi", NM: "New Mexico", TX: "Texas", WA: "Washington" };
  const baseFilter = { start: raw.metadata.minStartDate || "", end: raw.metadata.maxStartDate || "" };
  const sectionSpecs = [
    ["Final charges", (stay) => stay.finalCharge, 8],
    ["MSC charges", (stay) => stay.mscCharge, 8],
    ["Sentencing", (stay) => sentenceBucket(stay.sentenceDays), 6],
    ["Release", (stay) => stay.releaseReason, 8],
  ];
  const filterSpecs = [
    createFilterSpec("gender", "gender"),
    createFilterSpec("birthYear", "birth year", {
      value: (stay) => (stay.birthYear == null ? null : String(stay.birthYear)),
      sort: (left, right) => Number(right.label) - Number(left.label),
    }),
    createFilterSpec("race", "race"),
    createFilterSpec("ethnicity", "ethnicity"),
    createFilterSpec("birthCountry", "birth country"),
    createFilterSpec("birthRegion", "birth region"),
    createFilterSpec("felon", "felon"),
  ];
  const filterSpecByLabel = indexBy(filterSpecs, "label");
  const filterSpecByKey = indexBy(filterSpecs, "key");
  const filterOptions = buildFilterOptions(raw.stays, filterSpecs);
  const dom = {
    years: document.getElementById("year-summary"),
    title: document.getElementById("detail-title"),
    metrics: document.getElementById("detail-metrics"),
    sections: document.getElementById("detail-sections"),
    list: document.getElementById("connection-list"),
    caption: document.getElementById("connection-caption"),
    reset: document.getElementById("reset-selection"),
    filterStart: document.getElementById("filter-start"),
    filterEnd: document.getElementById("filter-end"),
    filterQuery: document.getElementById("filter-query"),
    filterSuggestions: document.getElementById("filter-suggestions"),
    filterPills: document.getElementById("filter-pills"),
  };
  const map = L.map("map", { worldCopyJump: true, minZoom: 2 }).setView([23, -35], 2);
  const layers = {
    lines: L.layerGroup().addTo(map),
    nodes: L.layerGroup().addTo(map),
  };
  const graphCache = new Map();
  const summaryCache = new Map();
  const groupedNodes = Object.fromEntries(["state", "city", "facility"].map((level) => [level, buildGroupedNodes(level)]));
  const homeBounds = L.latLngBounds(facilities.map(({ lat, lon }) => [lat, lon]));
  const app = {
    level: "",
    node: null,
    corridor: null,
    open: new Set(),
    frame: 0,
    full: false,
    interacting: false,
    completingFilter: false,
    filterKey: "",
    startDate: baseFilter.start,
    endDate: baseFilter.end,
    filters: [],
    filteredStays: raw.stays,
    graph: null,
    summary: null,
  };

  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    maxZoom: 19,
  }).addTo(map);

  function indexBy(items, key) {
    return Object.fromEntries(items.map((item) => [item[key], item]));
  }

  function unique(items) {
    return [...new Set(items)];
  }

  function clamp(value, min = 0, max = 1) {
    return Math.min(max, Math.max(min, value));
  }

  function sumBy(items, pick) {
    return items.reduce((sum, item) => sum + pick(item), 0);
  }

  function fmt(value) {
    return number.format(value);
  }

  function pct(value, total = 1) {
    return `${(((value || 0) / (total || 1)) * 100).toFixed(1)}%`;
  }

  function hours(value) {
    return Number.isFinite(value) ? `${value.toFixed(1)}h` : "n/a";
  }

  function mean(values) {
    return values.length ? sumBy(values, (value) => value) / values.length : null;
  }

  function median(values) {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  }

  function sentenceBucket(days) {
    if (!Number.isFinite(days)) return "No sentence";
    if (days < 30) return "<30d";
    if (days < 180) return "30-179d";
    if (days < 365) return "180-364d";
    if (days < 365 * 5) return "1-5y";
    if (days < 365 * 10) return "5-10y";
    return ">10y";
  }

  function normalizeFilterValue(value) {
    if (value == null) return null;
    const text = String(value).trim();
    return text ? text.toLowerCase() : null;
  }

  function createFilterSpec(key, label, options = {}) {
    return {
      key,
      label,
      value: options.value || ((stay) => stay[key]),
      normalize: options.normalize || normalizeFilterValue,
      sort: options.sort || ((left, right) => (right.count - left.count) || left.label.localeCompare(right.label)),
    };
  }

  function buildFilterOptions(stays, specs) {
    return Object.fromEntries(
      specs.map((spec) => {
        const counts = new Map();
        stays.forEach((stay) => {
          const rawValue = spec.value(stay);
          const norm = spec.normalize(rawValue);
          if (!norm) return;
          const current = counts.get(norm);
          if (current) current.count += 1;
          else counts.set(norm, { label: String(rawValue).trim(), norm, count: 1 });
        });
        return [spec.key, [...counts.values()].sort(spec.sort)];
      })
    );
  }

  function topRows(stays, pick, limit = 8) {
    const counts = new Map();
    stays.forEach((stay) => {
      const key = pick(stay) || "Unspecified";
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    return [...counts.entries()]
      .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
      .slice(0, limit)
      .map(([label, count]) => ({ label, count, share: count / (stays.length || 1) }));
  }

  function summarizeStays(stays) {
    const hold = stays.map(({ holdHours }) => holdHours).filter(Number.isFinite);
    const total = stays.map(({ totalHours }) => totalHours).filter(Number.isFinite);
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
      sections: sectionSpecs.map(([title, pick, limit]) => ({ title, rows: topRows(stays, pick, limit) })),
    };
  }

  function summarizeIds(ids) {
    const key = ids.join("|");
    if (summaryCache.has(key)) return summaryCache.get(key);
    const summary = summarizeStays(ids.map((id) => stayById[id]));
    summaryCache.set(key, summary);
    return summary;
  }

  function normalizeRange(start, end) {
    const range = { start: start || "", end: end || "" };
    if (range.start && range.end && range.start > range.end) [range.start, range.end] = [range.end, range.start];
    return range;
  }

  function syncFilterInputs(start, end) {
    dom.filterStart.value = start;
    dom.filterEnd.value = end;
  }

  function filterKey() {
    return app.filters
      .map((filter) => `${filter.field}:${filter.norm}`)
      .sort()
      .join("|");
  }

  function activeFilterGroups() {
    const groups = new Map();
    app.filters.forEach((filter) => {
      if (!groups.has(filter.field)) groups.set(filter.field, new Set());
      groups.get(filter.field).add(filter.norm);
    });
    return groups;
  }

  function matchesFilters(stay, groups) {
    return [...groups.entries()].every(([field, values]) => {
      const spec = filterSpecByKey[field];
      const stayValue = spec ? spec.normalize(spec.value(stay)) : null;
      return stayValue && values.has(stayValue);
    });
  }

  function filteredStays() {
    const range = normalizeRange(app.startDate, app.endDate);
    const key = `${range.start}|${range.end}|${filterKey()}`;
    if (app.filterKey === key) return app.filteredStays;
    Object.assign(app, { ...range, filterKey: key });
    syncFilterInputs(range.start, range.end);
    const groups = activeFilterGroups();
    app.filteredStays = raw.stays.filter((stay) => {
      if (range.start && stay.startDate < range.start) return false;
      if (range.end && stay.startDate > range.end) return false;
      if (groups.size && !matchesFilters(stay, groups)) return false;
      return true;
    });
    return app.filteredStays;
  }

  function yearCounts(stays) {
    const counts = Object.fromEntries((raw.metadata.years || []).map((year) => [String(year), 0]));
    stays.forEach(({ startDate }) => {
      const year = startDate?.slice(0, 4);
      if (year && year in counts) counts[year] += 1;
    });
    return counts;
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function rankMatches(items, query, label) {
    const text = query.trim().toLowerCase();
    if (!text) return items;
    return items
      .filter((item) => label(item).toLowerCase().includes(text))
      .sort((left, right) => {
        const leftLabel = label(left).toLowerCase();
        const rightLabel = label(right).toLowerCase();
        const leftStarts = leftLabel.startsWith(text) ? 0 : 1;
        const rightStarts = rightLabel.startsWith(text) ? 0 : 1;
        return leftStarts - rightStarts || leftLabel.localeCompare(rightLabel);
      });
  }

  function filterDraft(value = dom.filterQuery.value) {
    const input = String(value).trim();
    if (!input) return { stage: "field", field: null, query: "", suggestions: [] };

    const split = input.indexOf(":");
    if (split < 0) {
      const suggestions = rankMatches(filterSpecs, input, (spec) => spec.label).slice(0, 8);
      return { stage: "field", field: null, query: input, suggestions };
    }

    const label = input.slice(0, split).trim().toLowerCase();
    const field = filterSpecByLabel[label] || null;
    const query = input.slice(split + 1).trim();
    const suggestions = field ? rankMatches(filterOptions[field.key], query, (option) => option.label).slice(0, 8) : [];
    return { stage: "value", field, query, suggestions };
  }

  function suggestionItems(draft) {
    if (draft.stage === "field") return draft.query ? draft.suggestions : filterSpecs;
    if (!draft.field) return [];
    return draft.query ? draft.suggestions : filterOptions[draft.field.key];
  }

  function renderFilterSuggestions() {
    const draft = filterDraft();
    const items = suggestionItems(draft);
    dom.filterSuggestions.innerHTML = items.length
      ? `<div class="suggestion-list">${items
          .slice(0, 12)
          .map((item) =>
            draft.stage === "field"
              ? `<button class="suggestion" type="button" data-suggest-field="${escapeHtml(item.key)}">${escapeHtml(item.label)}</button>`
              : `<button class="suggestion" type="button" data-suggest-field="${escapeHtml(draft.field.key)}" data-suggest-value="${escapeHtml(item.label)}" data-suggest-norm="${escapeHtml(item.norm)}">${escapeHtml(item.label)}</button>`
          )
          .join("")}</div>`
      : "";
  }

  function exactField(text) {
    return filterSpecByLabel[String(text).trim().toLowerCase()] || null;
  }

  function exactFilterOption(field, text) {
    if (!field) return null;
    const norm = field.normalize(text);
    return norm ? filterOptions[field.key].find((option) => option.norm === norm) || null : null;
  }

  function startsWithQuery(label, query) {
    return String(label).toLowerCase().startsWith(String(query).trim().toLowerCase());
  }

  function resolvedFilter(text) {
    const split = text.indexOf(":");
    if (split < 0) return null;
    const field = filterSpecByLabel[text.slice(0, split).trim().toLowerCase()];
    if (!field) return null;
    const query = text.slice(split + 1).trim();
    if (!query) return null;
    const option = exactFilterOption(field, query);
    return option ? { field: field.key, fieldLabel: field.label, value: option.label, norm: option.norm } : null;
  }

  function autocompleteFilterInput(event) {
    if (app.completingFilter) return renderFilterUi();
    if (event?.inputType?.startsWith("delete")) return renderFilterUi();

    const input = dom.filterQuery;
    const rawValue = input.value;
    const start = input.selectionStart ?? rawValue.length;
    const end = input.selectionEnd ?? rawValue.length;
    if (start !== end || end !== rawValue.length) return renderFilterUi();

    const draft = filterDraft(rawValue);
    let completed = null;

    if (draft.stage === "field" && draft.query && draft.suggestions.length === 1) {
      const suggestion = draft.suggestions[0];
      if (suggestion && startsWithQuery(suggestion.label, draft.query) && suggestion.label.toLowerCase() !== rawValue.trim().toLowerCase()) {
        completed = { value: suggestion.label, start: rawValue.trim().length, end: suggestion.label.length };
      }
    } else if (draft.stage === "value" && draft.field && draft.query && draft.suggestions.length === 1) {
      const suggestion = draft.suggestions[0];
      if (suggestion && startsWithQuery(suggestion.label, draft.query)) {
        const prefix = `${draft.field.label}: `;
        const value = `${prefix}${suggestion.label}`;
        const typedLength = prefix.length + draft.query.length;
        if (value.toLowerCase() !== rawValue.trim().toLowerCase()) completed = { value, start: typedLength, end: value.length };
      }
    }

    if (completed) {
      app.completingFilter = true;
      input.value = completed.value;
      input.setSelectionRange(completed.start, completed.end);
      app.completingFilter = false;
    }

    renderFilterUi();
  }

  function unwindFilterBackspace() {
    const input = dom.filterQuery;
    const value = input.value;
    const start = input.selectionStart ?? value.length;
    const end = input.selectionEnd ?? value.length;
    if (start !== end || end !== value.length) return false;

    const field = exactField(value.replace(/:\s*$/, ""));
    if (!field) return false;
    const prefix = `${field.label}:`;
    if (!value.startsWith(prefix) || value.slice(prefix.length).trim()) return false;

    input.value = field.label;
    input.setSelectionRange(field.label.length, field.label.length);
    renderFilterUi();
    return true;
  }

  function acceptFilterTab() {
    const draft = filterDraft();
    if (draft.stage === "field") {
      const field = exactField(dom.filterQuery.value) || draft.suggestions[0];
      if (!field) return false;
      dom.filterQuery.value = `${field.label}: `;
      dom.filterQuery.setSelectionRange(dom.filterQuery.value.length, dom.filterQuery.value.length);
      renderFilterUi();
      return true;
    }

    const option = exactFilterOption(draft.field, draft.query) || draft.suggestions[0];
    if (!draft.field || !option) return false;
    dom.filterQuery.value = `${draft.field.label}: ${option.label}`;
    dom.filterQuery.setSelectionRange(dom.filterQuery.value.length, dom.filterQuery.value.length);
    renderFilterUi();
    return true;
  }

  function renderFilterUi() {
    dom.filterPills.innerHTML = app.filters
      .map(
        (filter, index) => `
          <span class="filter-pill">
            <span>${escapeHtml(filter.fieldLabel)}: ${escapeHtml(filter.value)}</span>
            <button type="button" data-filter-index="${index}" aria-label="Remove filter">&times;</button>
          </span>
        `
      )
      .join("");
    renderFilterSuggestions();
  }

  function cityKey(state, city) {
    return state && city ? `${state}|${city}` : null;
  }

  function splitCityKey(value) {
    const [state, ...rest] = String(value || "").split("|");
    return { state, city: rest.join("|") };
  }

  function cityLabel(city, state) {
    return cityCounts[city] > 1 ? `${city}, ${state}` : city;
  }

  function groupId(node, level) {
    if (node.kind === "country") return node.id;
    if (level === "facility") return node.id;
    return level === "city" ? `CITY:${node.state}:${node.city}` : `STATE:${node.state}`;
  }

  function groupLabel(node, level) {
    if (node.kind === "country" || level === "facility") return node.label;
    return level === "city" ? cityLabel(node.city, node.state) : (stateNames[node.state] || node.state);
  }

  function buildGroupedNodes(level) {
    const grouped = new Map();

    raw.nodes.forEach((node) => {
      const id = groupId(node, level);
      if (!grouped.has(id)) {
        grouped.set(id, {
          id,
          kind: node.kind === "country" ? "country" : level,
          label: groupLabel(node, level),
          lat: 0,
          lon: 0,
          count: 0,
          members: [],
          stateKey: node.kind === "country" ? null : node.state,
          cityKey: node.kind === "facility" || level === "city" ? cityKey(node.state, node.city) : null,
        });
      }
      const entry = grouped.get(id);
      entry.lat += node.lat;
      entry.lon += node.lon;
      entry.count += 1;
      entry.members.push(node.id);
    });

    return Object.fromEntries(
      [...grouped.values()].map((entry) => [
        entry.id,
        {
          id: entry.id,
          kind: entry.kind,
          label: entry.label,
          lat: entry.lat / entry.count,
          lon: entry.lon / entry.count,
          members: entry.members,
          stateKey: entry.stateKey,
          cityKey: entry.cityKey,
          moveCount: 0,
          stayCount: 0,
          stayIds: [],
          progressSum: 0,
          progressCount: 0,
          progressMean: 0,
          summary: summarizeIds([]),
        },
      ])
    );
  }

  function orderedPair(left, right) {
    return left < right ? [left, right] : [right, left];
  }

  function ensureCorridor(corridors, nodes, fromId, toId) {
    const [a, b] = orderedPair(fromId, toId);
    const key = `${a}__${b}`;
    if (!corridors.has(key)) {
      corridors.set(key, {
        key,
        a,
        b,
        label: `${nodes[a].label} - ${nodes[b].label}`,
        total: 0,
        forward: 0,
        backward: 0,
        progressSum: 0,
        progressCount: 0,
        stayIds: new Set(),
        segments: [],
      });
    }
    return corridors.get(key);
  }

  function addCorridor(corridors, nodes, fromId, toId, stayId, legRatio, segment) {
    const corridor = ensureCorridor(corridors, nodes, fromId, toId);
    corridor.total += 1;
    corridor[fromId === corridor.a ? "forward" : "backward"] += 1;
    corridor.progressSum += legRatio;
    corridor.progressCount += 1;
    corridor.stayIds.add(stayId);
    if (segment) corridor.segments.push(segment);
  }

  function finalizeNodes(nodes) {
    Object.values(nodes).forEach((node) => {
      node.stayIds = [...node.stayIds];
      node.stayCount = node.stayIds.length;
      node.progressMean = node.progressCount ? node.progressSum / node.progressCount : 0;
      node.summary = summarizeIds(node.stayIds);
    });
    return nodes;
  }

  function finalizeCorridors(corridors) {
    const list = [...corridors.values()]
      .map((corridor) => {
        const stayIds = [...corridor.stayIds];
        return {
          ...corridor,
          stayIds,
          progressMean: corridor.progressCount ? corridor.progressSum / corridor.progressCount : 0,
          summary: summarizeIds(stayIds),
        };
      })
      .sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));

    return {
      list,
      map: indexBy(list, "key"),
      maxTotal: Math.max(1, ...list.map((corridor) => corridor.total)),
    };
  }

  function forEachLeg(path, visit) {
    for (let index = 0; index < path.length - 1; index += 1) visit(path[index], path[index + 1], index);
  }

  function baseGraph(level, stays) {
    const cacheKey = `${level}|${app.filterKey}`;
    const cached = graphCache.get(level);
    if (cached?.key === cacheKey) return cached.graph;

    const nodes = Object.fromEntries(
      Object.entries(groupedNodes[level]).map(([id, node]) => [
        id,
        { ...node, moveCount: 0, stayIds: new Set(), progressSum: 0, progressCount: 0 },
      ])
    );
    const corridors = new Map();

    stays.forEach((stay) => {
      const legs = Math.max(stay.path.length - 1, 0);

      stay.path.forEach((rawId, index) => {
        const node = nodes[groupId(rawNodeById[rawId], level)];
        if (!node) return;
        node.stayIds.add(stay.id);
        node.progressSum += legs ? index / legs : 0;
        node.progressCount += 1;
      });

      forEachLeg(stay.path, (fromRaw, toRaw, index) => {
        const fromId = groupId(rawNodeById[fromRaw], level);
        const toId = groupId(rawNodeById[toRaw], level);
        if (!fromId || !toId || fromId === toId) return;
        const legRatio = legs ? (index + 1) / legs : 0;
        addCorridor(corridors, nodes, fromId, toId, stay.id, legRatio, { stayId: stay.id, fromId, toId, legRatio });
        nodes[fromId].moveCount += 1;
        nodes[toId].moveCount += 1;
      });
    });

    finalizeNodes(nodes);
    const finalized = finalizeCorridors(corridors);
    const graph = { level, nodes, corridors: finalized.list, corridorMap: finalized.map, maxTotal: finalized.maxTotal };
    graphCache.set(level, { key: cacheKey, graph });
    return graph;
  }

  function screenThreshold() {
    const { x, y } = map.getSize();
    return Math.max(56, Math.min(x, y) * 0.1);
  }

  function pointDistance(left, right) {
    return map.latLngToLayerPoint([left.lat, left.lon]).distanceTo(map.latLngToLayerPoint([right.lat, right.lon]));
  }

  function clusterLabel(parts, stateKey, cityValue) {
    if (parts.length === 1) return parts[0].label;
    if (parts.every((part) => part.kind === "country")) return `${parts.length} countries`;
    if (cityValue) {
      const { state, city } = splitCityKey(cityValue);
      return cityLabel(city, state);
    }
    if (stateKey) return stateNames[stateKey] || stateKey;
    return `${parts[0].label} + ${parts.length - 1}`;
  }

  function clusterFromMembers(memberIds, nodes) {
    const members = unique(memberIds).sort();
    const parts = members.map((id) => nodes[id]);
    const stateKeys = unique(parts.map((part) => part.stateKey).filter(Boolean));
    const cityKeys = unique(parts.map((part) => part.cityKey).filter(Boolean));
    const weight = sumBy(parts, (part) => Math.max(1, part.members.length));
    const stateKey = stateKeys.length === 1 ? stateKeys[0] : null;
    const cityValue = cityKeys.length === 1 ? cityKeys[0] : null;

    return {
      id: `NODE:${members.join("|")}`,
      members,
      lat: sumBy(parts, (part) => part.lat * Math.max(1, part.members.length)) / weight,
      lon: sumBy(parts, (part) => part.lon * Math.max(1, part.members.length)) / weight,
      stateKey,
      cityKey: cityValue,
      kind: parts.every((part) => part.kind === "country") ? "country" : cityValue ? "city" : stateKey ? "state" : "cluster",
      label: clusterLabel(parts, stateKey, cityValue),
    };
  }

  function mergeClusters(groups, keyName, nodes) {
    const parents = Object.fromEntries(groups.map(({ id }) => [id, id]));
    const root = (id) => (parents[id] === id ? id : (parents[id] = root(parents[id])));
    const join = (left, right) => {
      const a = root(left);
      const b = root(right);
      if (a !== b) parents[b] = a;
    };
    const buckets = new Map();
    const isPinned = (group) => group.members.some((id) => pinnedNodes.has(id));

    groups.forEach((group) => {
      const value = group[keyName];
      if (!value) return;
      if (!buckets.has(value)) buckets.set(value, []);
      buckets.get(value).push(group);
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
      const id = root(group.id);
      if (!merged.has(id)) merged.set(id, []);
      merged.get(id).push(...group.members);
    });

    return [...merged.values()].map((memberIds) => clusterFromMembers(memberIds, nodes));
  }

  function collapsedNodes(graph) {
    let groups = Object.values(graph.nodes).map((node) => clusterFromMembers([node.id], graph.nodes));
    const mergeKeys = graph.level === "facility" ? ["cityKey", "stateKey"] : graph.level === "city" ? ["stateKey"] : [];
    mergeKeys.forEach((key) => {
      groups = mergeClusters(groups, key, graph.nodes);
    });
    return groups;
  }

  function viewGraph(graph) {
    const nodeByBase = {};
    const nodes = {};

    collapsedNodes(graph).forEach((cluster) => {
      cluster.members.forEach((id) => {
        nodeByBase[id] = cluster.id;
      });
      const stayIds = unique(cluster.members.flatMap((id) => graph.nodes[id].stayIds));
      const progressSum = sumBy(cluster.members, (id) => graph.nodes[id].progressSum);
      const progressCount = sumBy(cluster.members, (id) => graph.nodes[id].progressCount);
      nodes[cluster.id] = {
        ...cluster,
        stayIds,
        stayCount: stayIds.length,
        moveCount: 0,
        progressSum,
        progressCount,
        progressMean: progressCount ? progressSum / progressCount : 0,
        summary: summarizeIds(stayIds),
      };
    });

    const corridors = new Map();
    graph.corridors.forEach((corridor) => {
      corridor.segments.forEach((segment) => {
        const fromId = nodeByBase[segment.fromId];
        const toId = nodeByBase[segment.toId];
        if (!fromId || !toId || fromId === toId) return;
        addCorridor(corridors, nodes, fromId, toId, segment.stayId, segment.legRatio, segment);
      });
    });

    const finalized = finalizeCorridors(corridors);
    finalized.list.forEach((corridor) => {
      nodes[corridor.a].moveCount += corridor.total;
      nodes[corridor.b].moveCount += corridor.total;
    });

    return {
      level: graph.level,
      base: graph,
      nodes,
      corridors: finalized.list,
      corridorMap: finalized.map,
      maxTotal: finalized.maxTotal,
    };
  }

  function corridorRoutes(corridor, graph) {
    if (corridor.routes) return corridor.routes;
    const routes = new Map();

    corridor.segments.forEach(({ stayId, fromId, toId }) => {
      const from = graph.base.nodes[fromId];
      const to = graph.base.nodes[toId];
      if (!from || !to || from.id === to.id) return;
      const [left, right] = from.id < to.id ? [from, to] : [to, from];
      const key = `${left.id}__${right.id}`;
      if (!routes.has(key)) routes.set(key, { key, label: `${left.label} - ${right.label}`, total: 0, stayIds: new Set() });
      const route = routes.get(key);
      route.total += 1;
      route.stayIds.add(stayId);
    });

    corridor.routes = [...routes.values()]
      .map((route) => {
        const stayIds = [...route.stayIds];
        return { ...route, stayIds, summary: summarizeIds(stayIds) };
      })
      .sort((a, b) => b.total - a.total || a.label.localeCompare(b.label));

    return corridor.routes;
  }

  function currentLevel() {
    const bounds = map.getBounds().pad(0.05);
    const visibleCountries = raw.nodes.filter((node) => node.kind === "country" && bounds.contains([node.lat, node.lon])).length;
    const visibleFacilities = facilities.filter((node) => bounds.contains([node.lat, node.lon]));
    const visibleStates = new Set(visibleFacilities.map(({ state }) => state)).size;
    const visibleCities = new Set(visibleFacilities.map(({ state, city }) => cityKey(state, city))).size;
    const zoom = map.getZoom();

    if (visibleCountries > 1 || zoom <= 3) return "state";
    return zoom >= 7 && visibleStates <= 2 && visibleCities <= 10 ? "facility" : "city";
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
    return rows.map((row) => `<li><span>${row.label}</span><span>${fmt(row.count)} | ${pct(row.share)}</span></li>`).join("");
  }

  function sectionBlocks(sections) {
    return sections
      .filter(({ rows }) => rows.length)
      .map(
        ({ title, rows }) => `
          <section class="block">
            <header><h3>${title}</h3></header>
            <ul>${sectionRows(rows)}</ul>
          </section>
        `
      )
      .join("");
  }

  function setDetail(title, metrics, sections) {
    dom.title.textContent = title;
    dom.metrics.innerHTML = metrics;
    dom.sections.innerHTML = sections;
  }

  function locationRows(node, graph) {
    const members = node.members
      .map((id) => graph.base.nodes[id])
      .sort((a, b) => b.stayCount - a.stayCount || a.label.localeCompare(b.label))
      .slice(0, 8);
    const total = sumBy(members, (member) => member.stayCount) || 1;
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

  function renderOverview(summary) {
    setDetail(
      "Overview",
      metricCards(summary, { label: "detentions", value: fmt(summary.stats.stays) }, { label: "exits", value: fmt(summary.stats.countryExits) }),
      sectionBlocks(summary.sections)
    );
  }

  function renderNode(node, graph) {
    const sections = [];
    if (node.members.length > 1) sections.push({ title: "Locations", rows: locationRows(node, graph) });
    const connections = connectionRows(node, graph);
    if (connections.length) sections.push({ title: "Connections", rows: connections });
    sections.push(...node.summary.sections);

    setDetail(
      node.label,
      metricCards(node.summary, { label: "detentions", value: fmt(node.summary.stats.stays) }, { label: "moves", value: fmt(node.moveCount) }),
      sectionBlocks(sections)
    );
  }

  function renderCorridor(corridor, graph) {
    setDetail(
      corridor.label,
      metricCards(corridor.summary, { label: "moves", value: fmt(corridor.total) }, { label: "detentions", value: fmt(corridor.summary.stats.stays) }),
      corridorRoutes(corridor, graph)
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
                        { label: "detentions", value: fmt(route.summary.stats.stays) }
                      )}</div>
                      ${sectionBlocks(route.summary.sections)}
                    </div>`
                  : ""
              }
            </section>
          `;
        })
        .join("")
    );
  }

  function renderYears(stays) {
    dom.years.innerHTML = Object.entries(yearCounts(stays))
      .map(([year, count]) => `<span class="chip">${year}: ${fmt(count)}</span>`)
      .concat(`<span class="chip">Detentions: ${fmt(stays.length)}</span>`)
      .join("");
  }

  function renderList(graph) {
    dom.caption.textContent = `${graph.level} view | ${fmt(graph.corridors.length)} shown`;
    dom.list.innerHTML = graph.corridors
      .slice(0, 20)
      .map(
        (corridor) => `
          <button class="item${corridor.key === app.corridor ? " active" : ""}" type="button" data-corridor="${corridor.key}">
            <span>${corridor.label}</span>
            <strong>${fmt(corridor.total)}</strong>
          </button>
        `
      )
      .join("");
  }

  function renderDetail(graph, summary) {
    const corridor = app.corridor && graph.corridorMap[app.corridor];
    if (corridor) return renderCorridor(corridor, graph);
    const node = app.node && graph.nodes[app.node];
    if (node) return renderNode(node, graph);
    return renderOverview(summary);
  }

  function mixColor(start, end, ratio) {
    const value = start.map((channel, index) => Math.round(channel + (end[index] - channel) * clamp(ratio)));
    return `rgb(${value[0]}, ${value[1]}, ${value[2]})`;
  }

  function progressFill(progress) {
    return mixColor([216, 128, 45], [115, 77, 172], progress);
  }

  function progressStroke(progress) {
    return mixColor([166, 93, 28], [78, 48, 130], progress);
  }

  function lineWeight(total, maxTotal, active) {
    if (total < 2) return active ? 2.25 : 1;
    if (total < 5) return active ? 3 : 1.8;
    const floor = Math.log(5);
    const ceiling = Math.max(floor + 0.0001, Math.log(Math.max(5, maxTotal)));
    const scaled = 2.4 + clamp((Math.log(total) - floor) / (ceiling - floor)) * 6.6;
    return active ? scaled + 1.35 : scaled;
  }

  function lineStyle(total) {
    if (total < 2) return { dashArray: "1 10", lineCap: "round" };
    if (total < 5) return { dashArray: "8 8", lineCap: "butt" };
    return { dashArray: null, lineCap: "butt" };
  }

  function nodeRadius(volume, maxVolume, active) {
    if (maxVolume <= 1) return active ? 4.5 : 3.25;
    const scaled = 3.25 + clamp(Math.log(volume + 1) / Math.log(maxVolume + 1)) * 11.75;
    return active ? scaled + 1 : scaled;
  }

  function corridorTooltip(corridor, graph) {
    const from = graph.nodes[corridor.a];
    const to = graph.nodes[corridor.b];
    return [
      corridor.forward && `${from.label} -> ${to.label} ${fmt(corridor.forward)}`,
      corridor.backward && `${to.label} -> ${from.label} ${fmt(corridor.backward)}`,
    ]
      .filter(Boolean)
      .join("<br>");
  }

  function renderMap(graph) {
    layers.lines.clearLayers();
    layers.nodes.clearLayers();

    const activeCorridor = app.corridor && graph.corridorMap[app.corridor];
    const maxNodeVolume = Math.max(1, ...Object.values(graph.nodes).map((node) => node.moveCount + node.stayCount));

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
        const path = [
          [from.lat, from.lon],
          [to.lat, to.lon],
        ];

        L.polyline(path, {
          color: progressFill(corridor.progressMean),
          weight,
          opacity: active ? 0.92 : 0.58,
          lineJoin: "round",
          interactive: false,
          ...lineStyle(corridor.total),
        }).addTo(layers.lines);

        L.polyline(path, {
          color: "#000",
          weight: Math.max(14, weight + 10),
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
          .on("click", () => selectCorridor(graph, corridor.key))
          .addTo(layers.lines);
      });

    Object.values(graph.nodes).forEach((node) => {
      if (!node.moveCount && !node.stayCount) return;
      const active = node.id === app.node || (activeCorridor && (activeCorridor.a === node.id || activeCorridor.b === node.id));
      const fill = progressFill(node.progressMean);
      L.circleMarker([node.lat, node.lon], {
        radius: nodeRadius(node.moveCount + node.stayCount, maxNodeVolume, active),
        weight: active ? 2 : 1,
        color: active ? progressStroke(node.progressMean) : fill,
        fillColor: fill,
        fillOpacity: 0.85,
      })
        .on("click", () => selectNode(node.id))
        .addTo(layers.nodes);
    });
  }

  function syncView() {
    const stays = filteredStays();
    const level = currentLevel();
    const levelChanged = app.level !== level;
    const graph = viewGraph(baseGraph(level, stays));
    const summary = summarizeIds(stays.map((stay) => stay.id));

    app.level = level;
    app.graph = graph;
    app.summary = summary;

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
    const { stays, graph, summary, levelChanged, selectionChanged } = syncView();
    if (full || levelChanged || selectionChanged) {
      renderYears(stays);
      renderList(graph);
      renderDetail(graph, summary);
    }
    renderMap(graph);
  }

  function scheduleRender(full = false) {
    if (full) app.full = true;
    if (app.interacting || app.frame) return;
    app.frame = window.requestAnimationFrame(() => {
      const renderFull = app.full;
      app.frame = 0;
      app.full = false;
      render(renderFull);
    });
  }

  function cancelRender() {
    if (!app.frame) return;
    window.cancelAnimationFrame(app.frame);
    app.frame = 0;
  }

  function selectNode(id) {
    app.node = id;
    app.corridor = null;
    app.open = new Set();
    render(true);
  }

  function selectCorridor(graph, id) {
    const routes = graph.corridorMap[id] ? corridorRoutes(graph.corridorMap[id], graph) : [];
    app.corridor = id;
    app.node = null;
    app.open = new Set(routes.length === 1 ? [routes[0].key] : []);
    render(true);
  }

  function applyFilter(start, end, filters = app.filters) {
    Object.assign(app, normalizeRange(start, end), {
      filterKey: "",
      filters,
      node: null,
      corridor: null,
      open: new Set(),
    });
    renderFilterUi();
    render(true);
  }

  function addFilter(filter) {
    if (app.filters.some((item) => item.field === filter.field && item.norm === filter.norm)) {
      dom.filterQuery.value = "";
      renderFilterUi();
      return;
    }
    dom.filterQuery.value = "";
    applyFilter(app.startDate, app.endDate, [...app.filters, filter]);
    dom.filterQuery.focus();
  }

  function removeFilter(index) {
    applyFilter(
      app.startDate,
      app.endDate,
      app.filters.filter((_, current) => current !== index)
    );
  }

  function fitHome() {
    if (homeBounds.isValid()) map.fitBounds(homeBounds, { padding: [60, 60], maxZoom: 6 });
  }

  dom.list.addEventListener("click", (event) => {
    const button = event.target.closest("[data-corridor]");
    if (button && app.graph) selectCorridor(app.graph, button.dataset.corridor);
  });

  dom.sections.addEventListener("click", (event) => {
    const button = event.target.closest("[data-route]");
    if (!button) return;
    if (app.open.has(button.dataset.route)) app.open.delete(button.dataset.route);
    else app.open.add(button.dataset.route);
    renderDetail(app.graph, app.summary);
  });

  dom.filterPills.addEventListener("click", (event) => {
    const button = event.target.closest("[data-filter-index]");
    if (button) removeFilter(Number(button.dataset.filterIndex));
  });

  dom.filterSuggestions.addEventListener("click", (event) => {
    const button = event.target.closest("[data-suggest-field]");
    if (!button) return;
    const field = filterSpecByKey[button.dataset.suggestField];
    if (!field) return;

    if (!button.dataset.suggestValue) {
      dom.filterQuery.value = `${field.label}: `;
      dom.filterQuery.focus();
      dom.filterQuery.setSelectionRange(dom.filterQuery.value.length, dom.filterQuery.value.length);
      renderFilterUi();
      return;
    }

    addFilter({
      field: field.key,
      fieldLabel: field.label,
      value: button.dataset.suggestValue,
      norm: button.dataset.suggestNorm,
    });
  });

  dom.reset.addEventListener("click", () => {
    dom.filterQuery.value = "";
    applyFilter(baseFilter.start, baseFilter.end, []);
    fitHome();
  });
  dom.filterStart.addEventListener("change", () => applyFilter(dom.filterStart.value, dom.filterEnd.value));
  dom.filterEnd.addEventListener("change", () => applyFilter(dom.filterStart.value, dom.filterEnd.value));
  dom.filterQuery.addEventListener("input", autocompleteFilterInput);
  dom.filterQuery.addEventListener("keydown", (event) => {
    if (event.key === "Backspace" && unwindFilterBackspace()) {
      event.preventDefault();
      return;
    }
    if (event.key === "Tab") {
      if (acceptFilterTab()) {
        event.preventDefault();
      }
      return;
    }
    if (event.key === "Enter") {
      const filter = resolvedFilter(dom.filterQuery.value);
      if (filter) {
        event.preventDefault();
        addFilter(filter);
      }
    }
  });

  dom.filterStart.min = raw.metadata.minStartDate || "";
  dom.filterStart.max = raw.metadata.maxStartDate || "";
  dom.filterEnd.min = raw.metadata.minStartDate || "";
  dom.filterEnd.max = raw.metadata.maxStartDate || "";
  syncFilterInputs(baseFilter.start, baseFilter.end);
  renderFilterUi();

  map.on("zoomstart movestart", () => {
    app.interacting = true;
    cancelRender();
  });
  map.on("zoomend moveend", () => {
    app.interacting = false;
    scheduleRender(true);
  });
  map.on("resize", () => scheduleRender(true));

  fitHome();
  render(true);
})();
