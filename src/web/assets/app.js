"use strict";

/* =========================================================================
 * p2p web ui
 * Single-page vanilla-JS app. All state comes from GET /api/state and
 * WS /api/ws (primary). Rendering is split into small per-section
 * functions that are idempotent: each keeps a cache of the last JSON
 * payload it rendered and skips DOM work if nothing changed, so a
 * duplicate state push never causes flicker or clobbers in-progress
 * form input.
 * ========================================================================= */

(function () {
  var latestState = null;

  // Tracks which notices (by time+kind+text) have already been surfaced as a
  // toast, so re-renders (duplicate state pushes, unrelated re-renders) never
  // re-toast the same notice. `null` means "no baseline yet" — the next
  // batch of notices seeds the baseline without toasting (used for the very
  // first render and after every WS reconnect, so a reconnect never replays
  // toasts for the whole backlog).
  var seenNoticeKeys = null;

  /* ----------------------------- dom refs ------------------------------ */

  var el = {
    toastContainer: document.getElementById("toast-container"),

    roomIdBtn: document.getElementById("room-id-btn"),
    roomIdValue: document.getElementById("room-id-value"),
    roomSwitchBtn: document.getElementById("room-switch-btn"),
    roomSwitchPopover: document.getElementById("room-switch-popover"),
    roomList: document.getElementById("room-list"),
    roomListEmpty: document.getElementById("room-list-empty"),
    roomConnectForm: document.getElementById("room-connect-form"),
    roomConnectInput: document.getElementById("room-connect-input"),
    roomNewBtn: document.getElementById("room-new-btn"),
    nodeIdBtn: document.getElementById("node-id-btn"),
    nodeIdValue: document.getElementById("node-id-value"),
    peerCount: document.getElementById("peer-count"),
    connIndicator: document.getElementById("conn-indicator"),
    connText: document.getElementById("conn-text"),
    themeToggle: document.getElementById("theme-toggle"),

    pendingSection: document.getElementById("pending-section"),
    pendingCount: document.getElementById("pending-count"),
    pendingAuthList: document.getElementById("pending-auth-list"),
    pendingForwardList: document.getElementById("pending-forward-list"),
    pendingOutgoingList: document.getElementById("pending-outgoing-list"),

    forwardsTbody: document.getElementById("forwards-tbody"),
    forwardsEmpty: document.getElementById("forwards-empty"),
    addForwardForm: document.getElementById("add-forward-form"),
    afProto: document.getElementById("af-proto"),
    afLocal: document.getElementById("af-local"),
    afRemote: document.getElementById("af-remote"),
    afPeer: document.getElementById("af-peer"),

    peersList: document.getElementById("peers-list"),
    peersEmpty: document.getElementById("peers-empty"),

    trustTbody: document.getElementById("trust-tbody"),
    trustEmpty: document.getElementById("trust-empty"),

    eventsLog: document.getElementById("events-log"),
    eventsEmpty: document.getElementById("events-empty"),

    chatLog: document.getElementById("chat-log"),
    chatEmpty: document.getElementById("chat-empty"),
    chatForm: document.getElementById("chat-form"),
    chatInput: document.getElementById("chat-input"),
  };

  /* ----------------------------- helpers -------------------------------- */

  function mkEl(tag, opts, children) {
    var node = document.createElement(tag);
    opts = opts || {};
    if (opts.className) node.className = opts.className;
    if (opts.text !== undefined) node.textContent = opts.text;
    if (opts.title !== undefined) node.title = opts.title;
    if (opts.attrs) {
      for (var k in opts.attrs) {
        if (Object.prototype.hasOwnProperty.call(opts.attrs, k)) {
          node.setAttribute(k, opts.attrs[k]);
        }
      }
    }
    if (opts.onClick) node.addEventListener("click", opts.onClick);
    (children || []).forEach(function (c) {
      if (c) node.appendChild(c);
    });
    return node;
  }

  function clearChildren(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  function shortId(id, head, tail) {
    if (typeof id !== "string") return String(id);
    head = head || 8;
    tail = tail || 6;
    if (id.length <= head + tail + 1) return id;
    return id.slice(0, head) + "…" + id.slice(-tail);
  }

  function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard
        .writeText(text)
        .then(function () {
          toast("Copied to clipboard", "info");
        })
        .catch(function () {
          toast("Copy failed", "error");
        });
    } else {
      toast("Clipboard unavailable", "error");
    }
  }

  function toast(message, kind) {
    var t = mkEl("div", {
      className: "toast" + (kind === "error" ? " toast-error" : " toast-info"),
      text: message,
    });
    el.toastContainer.appendChild(t);
    setTimeout(function () {
      if (t.parentNode) t.parentNode.removeChild(t);
    }, 4000);
  }

  // Only skip a render if the relevant slice of state is byte-identical to
  // last time we rendered that section. Prevents flicker on duplicate
  // pushes while keeping each section independent so unrelated form state
  // (e.g. the add-forward inputs) is never disturbed.
  var renderCache = {};
  function unchanged(key, value) {
    var json = JSON.stringify(value);
    if (renderCache[key] === json) return true;
    renderCache[key] = json;
    return false;
  }

  function statusClass(status) {
    var s = (status || "").toLowerCase();
    if (["active", "connected", "up", "open", "ok", "ready", "listening"].indexOf(s) !== -1) {
      return "status-good";
    }
    // Real error states arrive as "error: <detail>", so match by prefix.
    if (
      s.indexOf("error") === 0 ||
      ["failed", "disconnected", "closed", "down", "stopped"].indexOf(s) !== -1
    ) {
      return "status-bad";
    }
    if (["pending", "connecting", "starting", "waiting"].indexOf(s) !== -1) {
      return "status-warn";
    }
    return "";
  }

  function formatEventValue(v) {
    if (v === null || v === undefined) return String(v);
    if (typeof v === "object") {
      try {
        return JSON.stringify(v);
      } catch (e) {
        return String(v);
      }
    }
    return String(v);
  }

  // Splits a node-scoped target into its base target and the pinned node
  // id, mirroring the Rust `split_node_scope` helper: split on the *last*
  // '@' so a base containing one still round-trips, and treat an empty
  // base or empty scope as "not actually scoped".
  // `"tcp:127.0.0.1:22@node-a"` -> `["tcp:127.0.0.1:22", "node-a"]`
  // `"tcp:127.0.0.1:22"` -> `["tcp:127.0.0.1:22", null]`
  function splitNodeScope(target) {
    if (typeof target !== "string") return [target, null];
    var idx = target.lastIndexOf("@");
    if (idx <= 0 || idx === target.length - 1) return [target, null];
    return [target.slice(0, idx), target.slice(idx + 1)];
  }

  // Displayed targets omit the redundant "proto:" prefix the backend keeps
  // in the raw target string (the proto is already shown in its own
  // column/label), and render a trailing node scope (see `splitNodeScope`)
  // as a readable " @ node" suffix. Display-only: callers must keep using
  // the untouched value for API payloads and the `fwd.id` for the delete
  // action.
  function displayTarget(target, proto) {
    if (typeof target !== "string" || !proto) return target;
    var parts = splitNodeScope(target);
    var base = parts[0];
    var scope = parts[1];
    var prefix = proto + ":";
    var shown = base.indexOf(prefix) === 0 ? base.slice(prefix.length) : base;
    return scope ? shown + " @ " + scope : shown;
  }

  function formatTimeValue(v) {
    var d = null;
    if (typeof v === "number") {
      // heuristics: seconds vs milliseconds epoch
      d = new Date(v > 1e12 ? v : v * 1000);
    } else if (typeof v === "string") {
      var parsed = new Date(v);
      if (!isNaN(parsed.getTime())) d = parsed;
    }
    if (d && !isNaN(d.getTime())) {
      return d.toLocaleTimeString();
    }
    return formatEventValue(v);
  }

  // Events have a generic/unknown shape; render every field as text only
  // (never via innerHTML) so nothing server-supplied can be interpreted as
  // markup, regardless of what keys are present.
  function formatEventLine(ev) {
    if (ev === null || typeof ev !== "object") return formatEventValue(ev);
    var keys = Object.keys(ev);
    var timeKey = keys.find(function (k) {
      return /^(ts|time|timestamp)$/i.test(k);
    });
    var typeKey = keys.find(function (k) {
      return /^(type|kind|event|action)$/i.test(k);
    });
    var parts = [];
    if (timeKey !== undefined) parts.push("[" + formatTimeValue(ev[timeKey]) + "]");
    if (typeKey !== undefined) parts.push(String(ev[typeKey]));
    keys.forEach(function (k) {
      if (k === timeKey || k === typeKey) return;
      parts.push(k + "=" + formatEventValue(ev[k]));
    });
    return parts.join(" ");
  }

  /* ------------------------------ network ------------------------------- */

  function apiUrl(path) {
    return path;
  }

  function postJSON(path, body) {
    return fetch(apiUrl(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(handleMutationResponse)
      .catch(function (e) {
        toast("Network error: " + e.message, "error");
        return false;
      });
  }

  function deleteJSON(path, body) {
    var opts = { method: "DELETE" };
    if (body !== undefined) {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(body);
    }
    return fetch(apiUrl(path), opts)
      .then(handleMutationResponse)
      .catch(function (e) {
        toast("Network error: " + e.message, "error");
        return false;
      });
  }

  function handleMutationResponse(res) {
    return res
      .json()
      .catch(function () {
        return null;
      })
      .then(function (data) {
        if (!res.ok || !data || data.ok === false) {
          var msg = data && data.error ? data.error : "Request failed (" + res.status + ")";
          toast(msg, "error");
          return false;
        }
        return true;
      });
  }

  // Like postJSON, but for POST /api/room specifically: that endpoint's
  // success response carries the actual room_id (which may differ from what
  // was requested, e.g. an empty/omitted room_id asks the server to
  // auto-generate one), and callers need that value to toast/confirm which
  // room was actually joined. postJSON/handleMutationResponse intentionally
  // discard the parsed body down to a boolean, so this duplicates just
  // enough of that logic to surface data.room_id instead.
  function postRoomJSON(body) {
    return fetch(apiUrl("/api/room"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(function (res) {
        return res
          .json()
          .catch(function () {
            return null;
          })
          .then(function (data) {
            if (!res.ok || !data || data.ok === false) {
              var msg = data && data.error ? data.error : "Request failed (" + res.status + ")";
              toast(msg, "error");
              return null;
            }
            return data.room_id || null;
          });
      })
      .catch(function (e) {
        toast("Network error: " + e.message, "error");
        return null;
      });
  }

  /* ------------------------------ ws layer ------------------------------ */

  var ws = null;
  var wsConnected = false;
  var reconnectDelay = 500;
  var RECONNECT_MAX = 8000;
  var reconnectTimer = null;
  var pollTimer = null;

  function wsUrl() {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + location.host + "/api/ws";
  }

  function setConnState(state) {
    // state: "live" | "reconnecting"
    el.connIndicator.classList.remove("live", "reconnecting");
    el.connIndicator.classList.add(state);
    el.connText.textContent = state === "live" ? "live" : "reconnecting…";
  }

  function connectWS() {
    try {
      ws = new WebSocket(wsUrl());
    } catch (e) {
      scheduleReconnect();
      return;
    }
    setConnState(wsConnected ? "live" : "reconnecting");

    ws.onopen = function () {
      wsConnected = true;
      reconnectDelay = 500;
      setConnState("live");
      stopPolling();
      // Fresh connection: the next state push is a full backlog, not
      // incremental activity, so don't toast any of it — just re-seed the
      // baseline from it.
      seenNoticeKeys = null;
    };

    ws.onmessage = function (evt) {
      var data;
      try {
        data = JSON.parse(evt.data);
      } catch (e) {
        return;
      }
      if (data && data.type === "state") {
        handleState(data);
      }
    };

    ws.onclose = function () {
      wsConnected = false;
      setConnState("reconnecting");
      startPolling();
      scheduleReconnect();
    };

    ws.onerror = function () {
      if (ws) ws.close();
    };
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connectWS();
    }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
  }

  function startPolling() {
    if (pollTimer) return;
    pollOnce();
    pollTimer = setInterval(pollOnce, 3000);
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function pollOnce() {
    if (wsConnected) return;
    fetch(apiUrl("/api/state"))
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        handleState(data);
      })
      .catch(function () {
        /* ignore; will retry on next tick */
      });
  }

  /* ---------------------------- state intake ----------------------------- */

  function handleState(data) {
    latestState = data;
    renderHeader(data);
    renderPending(data.pending_auth || [], data.pending_forwards || [], data.pending_outgoing || []);
    renderForwardsTable(data.forwards || []);
    renderPeerSelect(data.peers || []);
    renderPeersSection(data.peers || []);
    renderTrust(data.trust || []);
    renderEvents(data.events || [], data.notices || []);
    renderChat(data.chat || []);
    processNoticeToasts(data.notices || []);
    updateTitleBadge((data.pending_auth || []).length + (data.pending_forwards || []).length);
  }

  function noticeKey(n) {
    return n.time + "|" + n.kind + "|" + n.text;
  }

  // Toasts any notice that wasn't present the last time we looked (see
  // `seenNoticeKeys` above for the reconnect/first-load baseline rule).
  function processNoticeToasts(notices) {
    var currentKeys = {};
    notices.forEach(function (n) {
      var key = noticeKey(n);
      currentKeys[key] = true;
      if (seenNoticeKeys !== null && !seenNoticeKeys[key]) {
        if (n.kind === "error") {
          toast(n.text, "error");
        } else if (n.kind === "info") {
          toast(n.text, "info");
        }
      }
    });
    seenNoticeKeys = currentKeys;
  }

  function updateTitleBadge(pendingCount) {
    document.title = (pendingCount > 0 ? "(!) " : "") + "p2p";
  }

  /* ------------------------------ rendering ------------------------------ */

  function renderHeader(state) {
    if (unchanged("header", { room_id: state.room_id, node_id: state.node_id, peers: (state.peers || []).length })) {
      return;
    }
    el.roomIdValue.textContent = state.room_id || "—";
    el.roomIdBtn.title = "Click to copy room id: " + (state.room_id || "");
    el.nodeIdValue.textContent = shortId(state.node_id || "");
    el.nodeIdBtn.title = "Click to copy node id: " + (state.node_id || "");
    var count = (state.peers || []).length;
    el.peerCount.textContent = count + (count === 1 ? " peer" : " peers");
  }

  el.roomIdBtn.addEventListener("click", function () {
    if (latestState && latestState.room_id) copyToClipboard(latestState.room_id);
  });
  el.nodeIdBtn.addEventListener("click", function () {
    if (latestState && latestState.node_id) copyToClipboard(latestState.node_id);
  });

  function renderPending(auths, forwards, outgoing) {
    if (unchanged("pending", { auths: auths, forwards: forwards, outgoing: outgoing })) return;

    // Badge count and title "(!)" stay actionable-only (auths+forwards);
    // outgoing entries are a waiting display, not something the user acts on.
    var actionableTotal = auths.length + forwards.length;
    var total = actionableTotal + outgoing.length;
    el.pendingSection.classList.toggle("hidden", total === 0);
    el.pendingCount.textContent = actionableTotal > 0 ? String(actionableTotal) : "";

    clearChildren(el.pendingAuthList);
    auths.forEach(function (item) {
      el.pendingAuthList.appendChild(renderPendingAuthCard(item));
    });

    clearChildren(el.pendingForwardList);
    forwards.forEach(function (item) {
      el.pendingForwardList.appendChild(renderPendingForwardCard(item));
    });

    clearChildren(el.pendingOutgoingList);
    outgoing.forEach(function (item) {
      el.pendingOutgoingList.appendChild(renderPendingOutgoingCard(item));
    });
  }

  function renderPendingAuthCard(item) {
    var info = mkEl("div", { className: "info" }, [
      mkEl("div", { text: shortId(item.peer_id) + "  ·  " + item.proto, title: item.peer_id }),
      mkEl("div", { className: "sub", text: "key: " + item.forward_key }),
      mkEl("div", { className: "sub", text: "target: " + item.target_addr }),
    ]);

    var allowBtn = mkEl("button", {
      className: "btn btn-allow",
      text: "Allow",
      attrs: { type: "button" },
      onClick: function () {
        postJSON("/api/pending/auth/" + item.id, { decision: "allow" });
      },
    });
    var alwaysBtn = mkEl("button", {
      className: "btn btn-allow",
      text: "Always",
      attrs: { type: "button" },
      onClick: function () {
        postJSON("/api/pending/auth/" + item.id, { decision: "allow_always" });
      },
    });
    var denyBtn = mkEl("button", {
      className: "btn btn-deny",
      text: "Deny",
      attrs: { type: "button" },
      onClick: function () {
        if (confirm("Deny this connection request?")) {
          postJSON("/api/pending/auth/" + item.id, { decision: "deny" });
        }
      },
    });
    var neverBtn = mkEl("button", {
      className: "btn btn-deny",
      text: "Never",
      attrs: { type: "button" },
      onClick: function () {
        if (confirm("Deny and remember this decision (never allow)?")) {
          postJSON("/api/pending/auth/" + item.id, { decision: "deny_always" });
        }
      },
    });

    var actions = mkEl("div", { className: "actions" }, [allowBtn, alwaysBtn, denyBtn, neverBtn]);
    return mkEl("div", { className: "pending-card" }, [info, actions]);
  }

  function renderPendingForwardCard(item) {
    var info = mkEl("div", { className: "info" }, [
      mkEl("div", { text: shortId(item.peer_id) + "  ·  " + item.proto, title: item.peer_id }),
      mkEl("div", { className: "sub", text: "remote: " + item.remote_addr }),
      mkEl("div", { className: "sub", text: "target: " + displayTarget(item.target, item.proto) }),
    ]);

    var acceptBtn = mkEl("button", {
      className: "btn btn-allow",
      text: "Accept",
      attrs: { type: "button" },
      onClick: function () {
        postJSON("/api/pending/forward/" + item.id, { accept: true });
      },
    });
    var rejectBtn = mkEl("button", {
      className: "btn btn-deny",
      text: "Reject",
      attrs: { type: "button" },
      onClick: function () {
        postJSON("/api/pending/forward/" + item.id, { accept: false });
      },
    });

    var actions = mkEl("div", { className: "actions" }, [acceptBtn, rejectBtn]);
    return mkEl("div", { className: "pending-card" }, [info, actions]);
  }

  function renderPendingOutgoingCard(item) {
    var info = mkEl("div", { className: "info" }, [
      mkEl("div", { text: shortId(item.peer_id) + "  ·  " + item.proto, title: item.peer_id }),
      mkEl("div", { className: "sub", text: "local: " + item.local }),
      mkEl("div", { className: "sub", text: "remote: " + item.remote }),
      mkEl("div", { className: "sub waiting-note", text: "waiting for peer approval…" }),
    ]);

    return mkEl("div", { className: "pending-card waiting" }, [info]);
  }

  function renderForwardsTable(forwards) {
    if (unchanged("forwards", forwards)) return;

    clearChildren(el.forwardsTbody);
    el.forwardsEmpty.style.display = forwards.length === 0 ? "" : "none";

    forwards.forEach(function (fwd) {
      var sClass = statusClass(fwd.status);
      var statusCell = mkEl("td", {}, [
        mkEl("span", { className: "status-cell" }, [
          mkEl("span", { className: "status-dot" + (sClass ? " " + sClass : "") }),
          mkEl("span", { text: fwd.status || "" }),
        ]),
      ]);

      var deleteBtn = mkEl("button", {
        className: "btn btn-danger btn-ghost",
        text: "Delete",
        attrs: { type: "button" },
        onClick: function () {
          if (confirm("Delete this forward?")) {
            deleteJSON("/api/forwards/" + fwd.id);
          }
        },
      });

      var row = mkEl("tr", {}, [
        mkEl("td", { className: "mono", text: fwd.proto || "" }),
        mkEl("td", { text: fwd.direction || "" }),
        mkEl("td", { className: "mono", text: fwd.local || "" }),
        mkEl("td", { className: "mono", text: displayTarget(fwd.target, fwd.proto) || "" }),
        statusCell,
        mkEl("td", {}, [deleteBtn]),
      ]);
      el.forwardsTbody.appendChild(row);
    });
  }

  function renderPeerSelect(peers) {
    if (unchanged("peer-select", peers)) return;

    var previousValue = el.afPeer.value;
    clearChildren(el.afPeer);
    el.afPeer.appendChild(mkEl("option", { text: "— select peer —", attrs: { value: "" } }));
    peers.forEach(function (p) {
      el.afPeer.appendChild(
        mkEl("option", { text: shortId(p.id), attrs: { value: p.id, title: p.id } })
      );
    });

    var stillPresent = peers.some(function (p) {
      return p.id === previousValue;
    });
    if (stillPresent) {
      el.afPeer.value = previousValue;
    } else if (peers.length === 1) {
      el.afPeer.value = peers[0].id;
    } else {
      el.afPeer.value = "";
    }
  }

  function renderPeersSection(peers) {
    if (unchanged("peers", peers)) return;

    clearChildren(el.peersList);
    el.peersEmpty.style.display = peers.length === 0 ? "" : "none";

    peers.forEach(function (p) {
      var chip = mkEl("li", {}, [
        mkEl("button", {
          className: "peer-chip",
          text: shortId(p.id),
          title: p.id + " (click to copy)",
          attrs: { type: "button" },
          onClick: function () {
            copyToClipboard(p.id);
          },
        }),
      ]);
      el.peersList.appendChild(chip);
    });
  }

  function renderTrust(trust) {
    if (unchanged("trust", trust)) return;

    clearChildren(el.trustTbody);
    el.trustEmpty.style.display = trust.length === 0 ? "" : "none";

    trust.forEach(function (t) {
      var badge = mkEl("span", {
        className: "badge " + (t.decision === "allow" ? "badge-allow" : "badge-deny"),
        text: t.decision,
      });

      var removeBtn = mkEl("button", {
        className: "btn btn-danger btn-ghost",
        text: "Remove",
        attrs: { type: "button" },
        onClick: function () {
          if (confirm("Remove this trust entry?")) {
            deleteJSON("/api/trust", { peer_id: t.peer_id, forward_key: t.forward_key });
          }
        },
      });

      var row = mkEl("tr", {}, [
        mkEl("td", { className: "mono", text: shortId(t.peer_id), title: t.peer_id }),
        mkEl("td", { className: "mono", text: t.forward_key }),
        mkEl("td", {}, [badge]),
        mkEl("td", {}, [removeBtn]),
      ]);
      el.trustTbody.appendChild(row);
    });
  }

  var EVENTS_CAP = 100;

  // Renders auth events and forward-negotiation notices as one merged,
  // chronologically-ordered log. Both arrive from the backend already
  // sorted oldest-first with RFC3339 `time` strings, so a stable string sort
  // on `time` interleaves them correctly (equal timestamps keep insertion
  // order — events pushed before notices — since Array#sort is spec-stable).
  function renderEvents(events, notices) {
    var merged = events
      .map(function (ev) {
        return { time: ev.time, isNotice: false, item: ev };
      })
      .concat(
        notices.map(function (n) {
          return { time: n.time, isNotice: true, item: n };
        })
      );
    merged.sort(function (a, b) {
      if (a.time < b.time) return -1;
      if (a.time > b.time) return 1;
      return 0;
    });

    // Cap to the most recent EVENTS_CAP entries before comparing/rendering;
    // we take the tail and display it most-recent-first.
    var capped = merged.length > EVENTS_CAP ? merged.slice(merged.length - EVENTS_CAP) : merged;
    if (unchanged("events", capped)) return;

    clearChildren(el.eventsLog);
    el.eventsEmpty.style.display = capped.length === 0 ? "" : "none";

    // .events-log uses column-reverse layout, so appending in chronological
    // order renders most-recent-first visually without an extra reverse().
    capped.forEach(function (entry) {
      if (entry.isNotice) {
        var n = entry.item;
        var cls = "event-row" + (n.kind === "error" ? " event-error" : "");
        el.eventsLog.appendChild(
          mkEl("div", { className: cls, text: "[" + formatTimeValue(n.time) + "] " + n.text })
        );
      } else {
        el.eventsLog.appendChild(
          mkEl("div", { className: "event-row", text: formatEventLine(entry.item) })
        );
      }
    });
  }

  /* --------------------------- add forward form --------------------------- */

  el.addForwardForm.addEventListener("submit", function (e) {
    e.preventDefault();
    var proto = el.afProto.value;
    var local = el.afLocal.value.trim();
    var remote = el.afRemote.value.trim();
    var peerId = el.afPeer.value || null;

    if (!local || !remote) {
      toast("Local and remote address are required", "error");
      return;
    }

    postJSON("/api/forwards", { proto: proto, local: local, remote: remote, peer_id: peerId }).then(
      function (ok) {
        if (ok) {
          el.afLocal.value = "";
          el.afRemote.value = "";
        }
      }
    );
  });

  /* ----------------------------- room switch ------------------------------ */

  function openRoomSwitchPopover() {
    el.roomSwitchPopover.classList.remove("hidden");
    el.roomSwitchBtn.setAttribute("aria-expanded", "true");
    loadRoomList();
    // Bound only while open (rather than one permanent listener) so the
    // common case — popover closed — costs nothing on every click/keydown.
    document.addEventListener("mousedown", onRoomSwitchOutsideClick);
    document.addEventListener("keydown", onRoomSwitchKeydown);
  }

  function closeRoomSwitchPopover() {
    el.roomSwitchPopover.classList.add("hidden");
    el.roomSwitchBtn.setAttribute("aria-expanded", "false");
    document.removeEventListener("mousedown", onRoomSwitchOutsideClick);
    document.removeEventListener("keydown", onRoomSwitchKeydown);
  }

  function onRoomSwitchOutsideClick(e) {
    if (!el.roomSwitchPopover.contains(e.target) && e.target !== el.roomSwitchBtn) {
      closeRoomSwitchPopover();
    }
  }

  function onRoomSwitchKeydown(e) {
    if (e.key === "Escape") closeRoomSwitchPopover();
  }

  el.roomSwitchBtn.addEventListener("click", function () {
    if (el.roomSwitchPopover.classList.contains("hidden")) {
      openRoomSwitchPopover();
    } else {
      closeRoomSwitchPopover();
    }
  });

  // Always fetches fresh (never relies on cached state) so the list
  // reflects rooms used since the last state push, per the popover spec.
  function loadRoomList() {
    clearChildren(el.roomList);
    el.roomListEmpty.style.display = "none";
    el.roomList.appendChild(mkEl("div", { className: "room-list-loading", text: "Loading…" }));
    fetch(apiUrl("/api/rooms"))
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        renderRoomList((data && data.rooms) || []);
      })
      .catch(function () {
        clearChildren(el.roomList);
        el.roomList.appendChild(mkEl("div", { className: "room-list-loading", text: "Failed to load rooms" }));
      });
  }

  function renderRoomList(rooms) {
    clearChildren(el.roomList);
    el.roomListEmpty.style.display = rooms.length === 0 ? "" : "none";

    rooms.forEach(function (r) {
      var row = mkEl(
        "div",
        { className: "room-list-row", attrs: { role: "menuitem", tabindex: "0" }, title: r.room_id },
        [
          mkEl("span", { className: "room-list-id mono", text: r.room_id }),
          mkEl("span", { className: "room-list-time", text: formatTimeValue(r.last_used_ms) }),
        ]
      );
      row.addEventListener("click", function () {
        switchRoom(r.room_id);
      });
      el.roomList.appendChild(row);
    });
  }

  function setRoomSwitchBusy(busy) {
    el.roomSwitchBtn.disabled = busy;
    el.roomNewBtn.disabled = busy;
    var connectBtn = el.roomConnectForm.querySelector("button[type=submit]");
    if (connectBtn) connectBtn.disabled = busy;
  }

  // Shared by the recent-room list, the "connect to id" form, and "new
  // room": all three just POST /api/room with a different room_id (a real
  // one, a user-typed one, or null to ask the server to auto-generate).
  function switchRoom(roomId) {
    setRoomSwitchBusy(true);
    return postRoomJSON({ room_id: roomId }).then(function (actualRoomId) {
      setRoomSwitchBusy(false);
      if (actualRoomId) {
        toast("Switched to room " + actualRoomId, "info");
        closeRoomSwitchPopover();
      }
      return actualRoomId;
    });
  }

  el.roomNewBtn.addEventListener("click", function () {
    switchRoom(null);
  });

  el.roomConnectForm.addEventListener("submit", function (e) {
    e.preventDefault();
    var val = el.roomConnectInput.value.trim();
    if (!val) {
      toast("Enter a room id", "error");
      return;
    }
    switchRoom(val).then(function (actualRoomId) {
      if (actualRoomId) el.roomConnectInput.value = "";
    });
  });

  /* -------------------------------- chat ----------------------------------- */

  function isNearBottom(node) {
    return node.scrollHeight - node.scrollTop - node.clientHeight < 30;
  }

  function renderChat(chat) {
    if (unchanged("chat", chat)) return;

    var wasNearBottom = isNearBottom(el.chatLog);

    clearChildren(el.chatLog);
    el.chatEmpty.style.display = chat.length === 0 ? "" : "none";

    chat.forEach(function (msg) {
      var bubble = mkEl("div", { className: "chat-bubble" }, [
        msg.mine ? null : mkEl("div", { className: "chat-peer", text: shortId(msg.peer_id || ""), title: msg.peer_id || "" }),
        mkEl("div", { className: "chat-text", text: msg.text }),
        mkEl("div", { className: "chat-time", text: formatTimeValue(msg.time) }),
      ]);
      el.chatLog.appendChild(
        mkEl("div", { className: "chat-row " + (msg.mine ? "chat-row-mine" : "chat-row-theirs") }, [bubble])
      );
    });

    if (wasNearBottom) {
      el.chatLog.scrollTop = el.chatLog.scrollHeight;
    }
  }

  el.chatForm.addEventListener("submit", function (e) {
    e.preventDefault();
    var text = el.chatInput.value.trim();
    if (!text) return;

    postJSON("/api/chat", { text: text }).then(function (ok) {
      if (ok) el.chatInput.value = "";
    });
  });

  /* ------------------------------- theme --------------------------------- */

  // Guard: a stale cached index.html may not have the toggle button yet;
  // never let its absence prevent the boot section below from running.
  if (el.themeToggle) {
    el.themeToggle.addEventListener("click", function () {
      var current = document.documentElement.dataset.theme;
      var next = current === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      try {
        localStorage.setItem("p2p-theme", next);
      } catch (e) {}
    });
  }

  /* --------------------------------- boot --------------------------------- */

  setConnState("reconnecting");
  startPolling();
  connectWS();
})();
